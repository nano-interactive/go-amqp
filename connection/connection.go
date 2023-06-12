package connection

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/amqp091-go"
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
}

var ErrRetriesExhausted = errors.New("number of retries to acquire conenction exhausted")

type (
	Connection interface {
		io.Closer
		IsClosed() bool

		RawConnection() *amqp091.Connection
	}

	connection struct {
		cancel context.CancelFunc
		conn   atomic.Pointer[amqp091.Connection]
		*Config
		onBeforeConnectionReady OnReconnectingFunc
		onConnectionReady       OnConnectionReady
		onError                 OnErrorFunc
		once                    sync.Once
		closing                 atomic.Bool
	}

	Config struct {
		Host              string        `json:"host,omitempty" mapstructure:"host" yaml:"host"`
		User              string        `json:"user,omitempty" mapstructure:"user" yaml:"user"`
		Password          string        `json:"password,omitempty" mapstructure:"password" yaml:"password"`
		Vhost             string        `json:"vhost,omitempty" mapstructure:"vhost" yaml:"vhost"`
		ConnectionName    string        `json:"connection_name,omitempty" mapstructure:"connection_name" yaml:"connection_name"`
		Port              int           `json:"port,omitempty" mapstructure:"port" yaml:"port"`
		ReconnectRetry    int           `json:"reconnect_retry,omitempty" mapstructure:"reconnect_retry" yaml:"reconnect_retry"`
		Channels          int           `json:"channels,omitempty" mapstructure:"channels" yaml:"channels"`
		ReconnectInterval time.Duration `json:"reconnect_interval,omitempty" mapstructure:"reconnect_interval" yaml:"reconnect_interval"`
	}

	Events struct {
		OnConnectionReady       OnConnectionReady  `json:"-" mapstructure:"-" yaml:"-"`
		OnBeforeConnectionReady OnReconnectingFunc `json:"-" mapstructure:"-" yaml:"-"`
		OnError                 OnErrorFunc        `json:"-" mapstructure:"-" yaml:"-"`
	}
)

func New(ctx context.Context, config Config, events Events) (Connection, error) {
	if events.OnConnectionReady == nil {
		return nil, fmt.Errorf("OnConnectionReady is required")
	}

	newCtx, cancel := context.WithCancel(ctx)
	c := &connection{
		Config:                  &config,
		cancel:                  cancel,
		onBeforeConnectionReady: events.OnBeforeConnectionReady,
		onConnectionReady:       events.OnConnectionReady,
		onError:                 events.OnError,
	}

	if err := c.connect(newCtx); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *connection) RawConnection() *amqp091.Connection {
	return c.conn.Load()
}

func (c *connection) IsClosed() bool {
	return c.conn.Load().IsClosed()
}

func (c *connection) handleReconnect(ctx context.Context, connection *amqp091.Connection) {
	notifyClose := connection.NotifyClose(make(chan *amqp091.Error))
	for {
		select {
		case <-ctx.Done():
			return
		case amqpErr, ok := <-notifyClose:
			if !ok {
				return
			}

			if c.closing.Load() {
				if err := c.connectionDispose(); err != nil && c.onError != nil {
					c.onError(&OnConnectionCloseError{Err: err})
				}

				return
			}

			// No need to reconnect if connection is not closed ( this error means that channel is closed)
			if errors.Is(amqpErr, amqp091.ErrClosed) && !c.conn.Load().IsClosed() {
				return
			}

			if c.onBeforeConnectionReady != nil {
				if err := c.onBeforeConnectionReady(ctx); err != nil && c.onError != nil {
					c.onError(&OnBeforeConnectError{Err: err})
				}
			}

			if err := c.connectionDispose(); err != nil && c.onError != nil {
				c.onError(&OnConnectionCloseError{Err: err})
			}

			var i int

			for i = 0; i < c.ReconnectRetry; i++ {
				if err := c.connect(ctx); err == nil {
					return
				}

				time.Sleep(c.ReconnectInterval)
			}

			if i >= c.ReconnectRetry {
				c.onError(ErrRetriesExhausted)
			}
		}
	}
}

func (c *connection) connect(ctx context.Context) error {
	// TODO Reuse properties to reduce allocations

	properties := amqp091.NewConnectionProperties()
	properties.SetClientConnectionName(c.ConnectionName)

	config := amqp091.Config{
		Vhost:      c.Vhost,
		ChannelMax: c.Channels,
		Heartbeat:  1 * time.Second,
		Properties: properties,
		Dial:       amqp091.DefaultDial(10 * time.Second),
	}

	// TODO: reuse connection URI
	connectionURI := fmt.Sprintf(
		"amqp://%s:%s@%s:%d",
		c.User,
		c.Password,
		c.Host,
		c.Port,
	)

	conn, err := amqp091.DialConfig(connectionURI, config)
	if err != nil {
		c.onError(&ConnectInitError{Err: err})
		return err
	}

	defer c.closing.Store(false)
	c.conn.Store(conn)

	go c.handleReconnect(ctx, conn)

	if err = c.onConnectionReady(ctx, conn); err != nil {
		if c.onError != nil {
			c.onError(&ConnectInitError{Err: err})
		}

		return err
	}

	return nil
}

func (c *connection) connectionDispose() error {
	c.closing.Store(true)
	conn := c.conn.Load()

	if conn.IsClosed() {
		return nil
	}

	return conn.Close()
}

func (c *connection) Close() error {
	var err error

	c.once.Do(func() {
		c.cancel()
		err = c.connectionDispose()
	})

	return err
}
