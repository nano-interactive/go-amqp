package publisher

import (
	"context"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/serializer"
)

type (
	Config[T any] struct {
		ctx               context.Context
		serializer        serializer.Serializer[T]
		onError           connection.OnErrorFunc
		exchange          ExchangeDeclare
		connectionOptions connection.Config
		messageBuffering  int
	}

	Option[T any] func(*Config[T])
)

func WithExchangeDeclare[T any](exchange ExchangeDeclare) Option[T] {
	return func(c *Config[T]) {
		c.exchange = exchange
	}
}

func WithSerializer[T any](ser serializer.Serializer[T]) Option[T] {
	return func(c *Config[T]) {
		c.serializer = ser
	}
}

func WithOnErrorFunc[T any](onError connection.OnErrorFunc) Option[T] {
	return func(c *Config[T]) {
		c.onError = onError
	}
}

func WithContext[T any](ctx context.Context) Option[T] {
	return func(c *Config[T]) {
		c.ctx = ctx
	}
}

func WithConnectionOptions[T any](connectionOptions connection.Config) Option[T] {
	return func(c *Config[T]) {
		if connectionOptions.Channels == 0 {
			connectionOptions.Channels = connection.DefaultConfig.Channels
		}

		if connectionOptions.Vhost == "" {
			connectionOptions.Vhost = connection.DefaultConfig.Vhost
		}

		if connectionOptions.Host == "" {
			connectionOptions.Host = connection.DefaultConfig.Host
		}

		if connectionOptions.Port == 0 {
			connectionOptions.Port = connection.DefaultConfig.Port
		}

		if connectionOptions.User == "" {
			connectionOptions.User = connection.DefaultConfig.User
		}

		if connectionOptions.Password == "" {
			connectionOptions.Password = connection.DefaultConfig.Password
		}

		if connectionOptions.ReconnectInterval == 0 {
			connectionOptions.ReconnectInterval = connection.DefaultConfig.ReconnectInterval
		}

		if connectionOptions.ReconnectRetry == 0 {
			connectionOptions.ReconnectRetry = connection.DefaultConfig.ReconnectRetry
		}

		c.connectionOptions = connectionOptions
	}
}

func WithBufferedMessages[T any](capacity int) Option[T] {
	return func(c *Config[T]) {
		c.messageBuffering = capacity
	}
}
