package connection

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
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
	FrameSize:         8192,
}

type (
	Connection struct {
		cancel                  context.CancelFunc
		conn                    atomic.Pointer[amqp091.Connection]
		config                  *Config
		onBeforeConnectionReady OnReconnectingFunc
		onConnectionReady       OnConnectionReady
		onError                 OnErrorFunc
		once                    func()
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
	}

	Events struct {
		OnConnectionReady       OnConnectionReady  `json:"-" mapstructure:"-" yaml:"-"`
		OnBeforeConnectionReady OnReconnectingFunc `json:"-" mapstructure:"-" yaml:"-"`
		OnError                 OnErrorFunc        `json:"-" mapstructure:"-" yaml:"-"`
	}
)

func New(ctx context.Context, config Config, events Events) (*Connection, error) {
	if events.OnConnectionReady == nil {
		return nil, ErrOnConnectionReady
	}

	ctx, cancel := context.WithCancel(ctx)

	c := &Connection{
		config:                  &config,
		cancel:                  cancel,
		onBeforeConnectionReady: events.OnBeforeConnectionReady,
		onConnectionReady:       events.OnConnectionReady,
		onError:                 events.OnError,
	}

	c.once = sync.OnceFunc(func() {
		c.cancel()
		c.connectionDispose()
	})

	return c.reconnect(ctx)
}

func (c *Connection) reconnect(ctx context.Context) (*Connection, error) {
	connect := c.connect()

	if err := connect(ctx); err == nil {
		return c, nil
	}

	var err error
	timer := time.NewTimer(c.config.ReconnectInterval)
	defer timer.Stop()

	for i := 0; i < c.config.ReconnectRetry; i++ {
		select {
		case <-timer.C:
			if err = connect(ctx); err == nil {
				return c, nil
			}

			timer.Reset(c.config.ReconnectInterval)
		case <-ctx.Done():
			return c, ctx.Err()
		}
	}

	return c, err
}

func (c *Connection) hasChannelClosed(err error) bool {
	return errors.Is(err, amqp091.ErrClosed) && !c.conn.Load().IsClosed()
}

func (c *Connection) IsClosed() bool {
	conn := c.conn.Load()
	return conn != nil && conn.IsClosed()
}

func (c *Connection) handleReconnect(ctx context.Context, connection *amqp091.Connection) {
	notifyClose := connection.NotifyClose(make(chan *amqp091.Error))

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

			if _, err := c.reconnect(ctx); err != nil {
				return
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
				return err
			}
		}

		conn, err := amqp091.DialConfig(connectionURI, config)
		if err != nil {
			c.onError(&ConnectInitError{Inner: err})
			return err
		}

		c.conn.Store(conn)

		go c.handleReconnect(ctx, conn)

		if err = c.onConnectionReady(ctx, conn); err != nil {
			if c.onError != nil {
				c.onError(&ConnectInitError{Inner: err})
			}

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
	c.once()

	return nil
}
