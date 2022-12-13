package connection

import (
	"fmt"
	"io"
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

type (
	Connection interface {
		io.Closer
		IsClosed() bool
		SetOnReconnect(onReconnect func() error)
		SetOnReconnecting(onReconnecting func() error)
		SetOnError(onReconnecting func(error))

		RawConnection() *amqp091.Connection
	}

	connection struct {
		conn atomic.Pointer[amqp091.Connection]
		*Config
		*Events
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

func New(config *Config) (Connection, error) {
	c := &connection{
		Config: config,
		conn:   atomic.Pointer[amqp091.Connection]{},
		Events: &Events{
			onReconnecting: nil,
			onReconnect:    nil,
			onError:        nil,
		},
	}

	if err := c.connect(); err != nil {
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

func (c *connection) handleErrors(ch chan *amqp091.Error) error {
	for amqpErr := range ch {
		if !shouldReconnect(amqpErr) {
			continue
		}

		if c.onReconnecting != nil {
			if err := c.onReconnecting(); err != nil {
				return err
			}
		}

		// Clean up previous connection
		if err := c.Close(); err != nil {
			return err
		}

		if amqpErr.Code != amqp091.ConnectionForced {
			close(ch)
		}

		var err error
		for i := 0; i < c.ReconnectRetry; i++ {
			if err = c.connect(); err == nil && !c.conn.Load().IsClosed() {
				if c.onReconnect != nil {
					if err := c.onReconnect(); err != nil {
						return err
					}
				}
				break
			}

			time.Sleep(c.ReconnectInterval)
		}

		if c.onError != nil && err != nil {
			c.onError(err)
		}
	}
	return nil
}

func (c *connection) connect() error {
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

	c.conn.Store(conn)

	go func() {
		if err := c.handleErrors(conn.NotifyClose(make(chan *amqp091.Error))); err != nil {
			panic(err)
		}
	}()

	return nil
}

func (c *connection) Close() error {
	conn := c.conn.Load()

	if !conn.IsClosed() {
		return conn.Close()
	}

	return nil
}
