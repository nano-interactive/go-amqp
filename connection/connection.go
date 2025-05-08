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

// String returns the string representation of the connection state
func (s State) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	case StateClosing:
		return "closing"
	default:
		return "unknown"
	}
}

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

var (
	ErrTimeoutCleanup = errors.New("timeout waiting for connection cleanup")
)

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

	conn, err := c.reconnect()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// setState updates the connection state in a thread-safe manner
func (c *Connection) setState(state State) {
	c.state.Store(int32(state))
}

// getState returns the current connection state in a thread-safe manner
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

// reconnect attempts to establish a connection with exponential backoff
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

			// Notify about the failed attempt
			if c.onError != nil {
				c.onError(err)
			}

			// Calculate next backoff with exponential increase and jitter
			backoff = c.calculateNextBackoff(backoff, maxBackoff)

		case <-ctx.Done():
			c.setState(StateClosing)
			return c, ctx.Err()
		}
	}

	// Final error after all retries
	if c.onError != nil {
		c.onError(err)
	}
	c.setState(StateDisconnected)
	return c, err
}

// calculateNextBackoff calculates the next backoff duration with exponential increase and jitter
func (c *Connection) calculateNextBackoff(current, max time.Duration) time.Duration {
	// Exponential backoff
	next := time.Duration(math.Min(
		float64(current*2),
		float64(max),
	))

	// Add jitter (±20%)
	jitter := time.Duration(float64(next) * 0.2)
	return next + secureJitter(jitter)
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
			c.setState(StateClosing)
			return
		case amqpErr, ok := <-notifyClose:
			if !ok {
				return
			}

			if c.hasChannelClosed(amqpErr) {
				continue
			}

			// First notify about the error
			if c.onError != nil {
				c.onError(amqpErr)
			}

			// Then update state and dispose
			c.setState(StateReconnecting)
			c.connectionDispose()

			// Attempt to reconnect
			if _, err := c.reconnect(); err != nil {
				if c.onError != nil {
					c.onError(err)
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
			if c.onError != nil {
				c.onError(&ConnectInitError{Inner: err})
			}
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

// connectionDispose safely closes the current connection and notifies about any errors
func (c *Connection) connectionDispose() {
	conn := c.conn.Load()
	if conn == nil || conn.IsClosed() {
		return
	}

	// Set up close notification
	closeChan := make(chan *amqp091.Error, 1)
	conn.NotifyClose(closeChan)

	// Attempt to close the connection
	if err := conn.Close(); err != nil && c.onError != nil {
		c.onError(err)
	}

	// Wait for close confirmation or timeout
	c.waitForClose(closeChan)
}

// waitForClose waits for the connection to close or times out
func (c *Connection) waitForClose(closeChan chan *amqp091.Error) {
	const closeTimeout = 5 * time.Second

	select {
	case amqpErr := <-closeChan:
		if amqpErr != nil && c.onError != nil {
			c.onError(amqpErr)
		}
	case <-time.After(closeTimeout):
		if c.onError != nil {
			c.onError(errors.New("timeout waiting for connection to close"))
		}
	}
}

func (c *Connection) Close() error {
	var err error

	c.once.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()

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
