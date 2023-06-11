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
		SetOnReconnect(onReconnect OnReconnectFunc)
		SetOnReconnecting(onReconnecting OnReconnectingFunc)
		SetOnError(onReconnecting OnErrorFunc)

		RawConnection() *amqp091.Connection
	}

	connection struct {
		cancel context.CancelFunc
		conn   atomic.Pointer[amqp091.Connection]
		*Config
		*Events
		once    sync.Once
		closing atomic.Bool
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
)

func New(ctx context.Context, config *Config) (Connection, error) {
	newCtx, cancel := context.WithCancel(ctx)
	c := &connection{
		Config: config,
		cancel: cancel,
		Events: &Events{
			onReconnecting: nil,
			onReconnect:    nil,
			onError:        nil,
		},
	}

	if err := c.connect(newCtx); err != nil {
		return nil, err
	}

	return c, nil
}

func shouldReconnect(amqpErr *amqp091.Error) bool {
	return amqpErr.Code == amqp091.ConnectionForced ||
		amqpErr.Code == amqp091.ResourceLocked ||
		amqpErr.Code == amqp091.PreconditionFailed ||
		amqpErr.Code == amqp091.ChannelError ||
		amqpErr.Code == amqp091.ResourceError
}

func (c *connection) RawConnection() *amqp091.Connection {
	return c.conn.Load()
}

func (c *connection) IsClosed() bool {
	return c.conn.Load().IsClosed()
}

func (c *connection) handleReconnect(ctx context.Context, connection *amqp091.Connection) {
	notifyClose := connection.NotifyClose(make(chan *amqp091.Error))
	for notifyClose != nil {
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
					continue
				}

				return
			}

			if !shouldReconnect(amqpErr) {
				return
			}

			if c.onReconnecting != nil {
				if err := c.onReconnecting(ctx); err != nil && c.onError != nil {
					c.onError(&OnReconnectingError{Err: err})
					continue
				}
			}

			if err := c.connectionDispose(); err != nil && c.onError != nil {
				c.onError(&OnConnectionCloseError{Err: err})
				continue
			}

			var (
				err error
				i   int
			)

			for i = 0; i < c.ReconnectRetry; i++ {
				if err = c.connect(ctx); err == nil && !c.closing.Load() && !c.conn.Load().IsClosed() {
					if c.onReconnect != nil {
						if err = c.onReconnect(ctx); err != nil && c.onError != nil {
							c.onError(&OnReconnectError{Err: err})
						}
					}
					break
				}

				time.Sleep(c.ReconnectInterval)
			}

			if c.onError != nil && err != nil {
				c.onError(err)
			}

			if i >= c.ReconnectRetry {
				c.onError(ErrRetriesExhausted)
			}
		}
	}
}

func (c *connection) connect(ctx context.Context) error {
	properties := amqp091.NewConnectionProperties()
	properties.SetClientConnectionName(c.ConnectionName)

	config := amqp091.Config{
		Vhost:      c.Vhost,
		ChannelMax: 0,
		Heartbeat:  1 * time.Second,
		Properties: properties,
		Dial:       amqp091.DefaultDial(10 * time.Second),
	}

	connectionURI := fmt.Sprintf(
		"amqp://%s:%s@%s:%d",
		c.User,
		c.Password,
		c.Host,
		c.Port,
	)

	conn, err := amqp091.DialConfig(connectionURI, config)
	if err != nil {
		return err
	}

	c.closing.Store(false)
	c.conn.Store(conn)

	go func() {
		c.handleReconnect(ctx, conn)
	}()

	return nil
}

func (c *connection) connectionDispose() error {
	c.closing.Store(true)
	conn := c.conn.Load()

	if !conn.IsClosed() {
		return conn.Close()
	}

	return nil
}

func (c *connection) Close() error {
	var err error

	c.once.Do(func() {
		c.cancel()
		err = c.connectionDispose()
	})

	return err
}
