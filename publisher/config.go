package publisher

import (
	"context"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/serializer"
)

type (
	Config[T any] struct {
		ctx               context.Context
		serializer        serializer.Serializer[T]
		logger            amqp.Logger
		onError           connection.OnErrorFunc
		exchange          ExchangeDeclare
		connectionOptions connection.Config
		messageBuffering  int
	}

	PublisherConfig struct{}

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
		c.connectionOptions = connectionOptions
	}
}

func WithBufferedMessages[T any](capacity int) Option[T] {
	return func(c *Config[T]) {
		c.messageBuffering = capacity
	}
}

func WithLogger[T any](logger amqp.Logger) Option[T] {
	return func(c *Config[T]) {
		if logger != nil {
			c.logger = logger
		}
	}
}
