package connection

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// State represents the current state of the connection
type State int32

const (
	StateDisconnected State = iota
	StateConnecting
	StateConnected
	StateReconnecting
	StateClosing
)

var DefaultConfig = Config{
	Host:              "127.0.0.1",
	User:              "guest",
	Password:          "guest",
	Vhost:             "/",
	Port:              5672,
	ConnectionName:    "go-amqp",
	ReconnectRetry:    10,
	Channels:          100,
	ReconnectInterval: 1 * time.Second,
	FrameSize:         8192,
	MaxBackoff:        30 * time.Second,
}

var ErrTimeoutCleanup = errors.New("timeout waiting for connection cleanup")

type (
	Connection struct {
		mu                      sync.Mutex
		base                    context.Context
		cancel                  context.CancelFunc
		conn                    atomic.Pointer[amqp091.Connection]
		config                  *Config
		onBeforeConnectionReady OnReconnectingFunc
		onConnectionReady       OnConnectionReady
		onError                 OnErrorFunc
		state                   atomic.Int32
		reconnectWg             sync.WaitGroup
		once                    sync.Once
	}

	Config struct {
		Host              string        `json:"host,omitempty" mapstructure:"host" yaml:"host"`
		User              string        `json:"user,omitempty" mapstructure:"user" yaml:"user"`
		Password          string        `json:"password,omitempty" mapstructure:"password" yaml:"password"`
		Vhost             string        `json:"vhost,omitempty" mapstructure:"vhost" yaml:"vhost"`
		ConnectionName    string        `json:"connection_name,omitempty" mapstructure:"connection_name" yaml:"connection_name"`
		Port              int           `json:"port,omitempty" mapstructure:"port" yaml:"port"`
		ReconnectRetry    int           `json:"reconnect_retry,omitempty" mapstructure:"reconnect_retry" yaml:"reconnect_retry"`
		Channels          uint16        `json:"channels,omitempty" mapstructure:"channels" yaml:"channels"`
		FrameSize         int           `json:"frame_size,omitempty" mapstructure:"frame_size" yaml:"frame_size"`
		ReconnectInterval time.Duration `json:"reconnect_interval,omitempty" mapstructure:"reconnect_interval" yaml:"reconnect_interval"`
		MaxBackoff        time.Duration `json:"max_backoff,omitempty" mapstructure:"max_backoff" yaml:"max_backoff"`
	}

	Events struct {
		OnConnectionReady       OnConnectionReady  `json:"-" mapstructure:"-" yaml:"-"`
		OnBeforeConnectionReady OnReconnectingFunc `json:"-" mapstructure:"-" yaml:"-"`
		OnError                 OnErrorFunc        `json:"-" mapstructure:"-" yaml:"-"`
	}

	// ReconnectError represents an error that occurred during reconnection
	ReconnectError struct {
		Inner error
	}

	// BlockedError represents when the connection is blocked by the broker
	BlockedError struct {
		Blocked amqp091.Blocking
	}
)

func New(ctx context.Context, config Config, events Events) (*Connection, error) {
	if events.OnConnectionReady == nil {
		return nil, ErrOnConnectionReady
	}

	c := &Connection{
		base:                    ctx,
		config:                  &config,
		onBeforeConnectionReady: events.OnBeforeConnectionReady,
		onConnectionReady:       events.OnConnectionReady,
		onError:                 events.OnError,
	}

	return c.reconnect()
}

func (c *Connection) setState(state State) {
	c.state.Store(int32(state))
}

func (c *Connection) getState() State {
	return State(c.state.Load())
}

// secureJitter returns a random jitter value between -jitter and +jitter using crypto/rand
func secureJitter(jitter time.Duration) time.Duration {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// If we can't get random bytes, return 0 jitter
		return 0
	}
	// Convert to float64 between 0 and 1
	f := float64(binary.BigEndian.Uint64(buf[:])) / float64(uint64(1<<63))
	// Convert to range [-1, 1]
	f = (f * 2) - 1
	return time.Duration(float64(jitter) * f)
}

func (c *Connection) reconnect() (*Connection, error) {
	connect := c.connect()
	var ctx context.Context

	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
	}
	ctx, c.cancel = context.WithCancel(c.base)
	c.mu.Unlock()

	// Initial connection attempt
	if err := connect(ctx); err == nil {
		c.setState(StateConnected)
		return c, nil
	}

	// Start reconnection process
	c.setState(StateReconnecting)

	var err error
	backoff := c.config.ReconnectInterval
	maxBackoff := c.config.MaxBackoff

	for i := 0; i < c.config.ReconnectRetry; i++ {
		select {
		case <-time.After(backoff):
			if err = connect(ctx); err == nil {
				c.setState(StateConnected)
				return c, nil
			}

			// Exponential backoff with secure jitter
			backoff = time.Duration(math.Min(
				float64(backoff*2),
				float64(maxBackoff),
			))
			// Add jitter (±20%)
			jitter := time.Duration(float64(backoff) * 0.2)
			backoff += secureJitter(jitter)

		case <-ctx.Done():
			c.setState(StateDisconnected)
			return c, ctx.Err()
		}
	}

	c.setState(StateDisconnected)
	return c, fmt.Errorf("reconnection failed after %d attempts: %w", c.config.ReconnectRetry, err)
}

func (c *Connection) hasChannelClosed(err error) bool {
	return errors.Is(err, amqp091.ErrClosed) && !c.conn.Load().IsClosed()
}

func (c *Connection) IsClosed() bool {
	conn := c.conn.Load()
	return conn != nil && conn.IsClosed()
}

func (c *Connection) handleReconnect(ctx context.Context, connection *amqp091.Connection) {
	c.reconnectWg.Add(1)
	defer c.reconnectWg.Done()

	notifyClose := connection.NotifyClose(make(chan *amqp091.Error, 1))
	notifyBlocked := connection.NotifyBlocked(make(chan amqp091.Blocking, 1))

	for {
		select {
		case <-ctx.Done():
			return
		case amqpErr, ok := <-notifyClose:
			if !ok {
				return
			}

			if c.hasChannelClosed(amqpErr) {
				continue
			}

			c.connectionDispose()
			c.setState(StateReconnecting)

			if _, err := c.reconnect(); err != nil {
				if c.onError != nil {
					c.onError(&ReconnectError{Inner: err})
				}
				return
			}
		case blocked, ok := <-notifyBlocked:
			if !ok {
				return
			}
			if c.onError != nil {
				c.onError(&BlockedError{Blocked: blocked})
			}
		}
	}
}

func (c *Connection) connect() func(ctx context.Context) error {
	connectionURI := fmt.Sprintf(
		"amqp://%s:%s@%s",
		c.config.User,
		c.config.Password,
		net.JoinHostPort(c.config.Host, strconv.FormatInt(int64(c.config.Port), 10)),
	)

	properties := amqp091.NewConnectionProperties()
	properties.SetClientConnectionName(c.config.ConnectionName)
	if err := properties.Validate(); err != nil {
		panic("Invalid connection properties: " + err.Error())
	}

	return func(ctx context.Context) error {
		c.connectionDispose()
		c.setState(StateConnecting)

		config := amqp091.Config{
			SASL:       nil,
			Vhost:      c.config.Vhost,
			ChannelMax: c.config.Channels,
			FrameSize:  c.config.FrameSize,
			Heartbeat:  3 * time.Second,
			Properties: properties,
			Dial:       amqp091.DefaultDial(c.config.ReconnectInterval),
		}

		if c.onBeforeConnectionReady != nil {
			if err := c.onBeforeConnectionReady(ctx); err != nil {
				if c.onError != nil {
					c.onError(&OnBeforeConnectError{Inner: err})
				}
				c.setState(StateDisconnected)
				return err
			}
		}

		conn, err := amqp091.DialConfig(connectionURI, config)
		if err != nil {
			c.onError(&ConnectInitError{Inner: err})
			c.setState(StateDisconnected)
			return err
		}

		c.conn.Store(conn)
		c.setState(StateConnected)

		go c.handleReconnect(ctx, conn)

		if err = c.onConnectionReady(ctx, conn); err != nil {
			if c.onError != nil {
				c.onError(&ConnectInitError{Inner: err})
			}
			c.setState(StateDisconnected)
			return err
		}

		return nil
	}
}

func (c *Connection) connectionDispose() {

	conn := c.conn.Load()
	if conn == nil || conn.IsClosed() {
		return
	}

	if err := conn.Close(); err != nil && c.onError != nil {
		c.onError(&OnConnectionCloseError{Inner: err})
	}
}

func (c *Connection) Close() error {
	var err error

	c.once.Do(func() {

		c.mu.Lock()
		defer c.mu.Unlock()

		if c.getState() == StateClosing {
			return
		}

		c.setState(StateClosing)

		if c.cancel != nil {
			c.cancel()
		}
		c.connectionDispose()

		// Wait for all reconnect goroutines to finish
		done := make(chan struct{})
		go func() {
			c.reconnectWg.Wait()
			close(done)
		}()

		// Wait with timeout
		select {
		case <-done:
			return
		case <-time.After(5 * time.Second):
			err = ErrTimeoutCleanup
			return
		}
	})
	return err
}

func (e *ReconnectError) Error() string {
	return fmt.Sprintf("reconnection failed: %v", e.Inner)
}

func (e *ReconnectError) Unwrap() error {
	return e.Inner
}

func (e *BlockedError) Error() string {
	return fmt.Sprintf("connection blocked: reason=%s, active=%v", e.Blocked.Reason, e.Blocked.Active)
}
