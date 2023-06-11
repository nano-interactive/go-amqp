package publisher

import (
	"context"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/serializer"
)

type (
	Config[T Message] struct {
		ctx               context.Context
		serializer        serializer.Serializer[T]
		logger            amqp.Logger
		onError           connection.OnErrorFunc
		connectionOptions connection.Config
		messageBuffering  int
	}

	Option[T Message] func(*Config[T])
)

func WithSerializer[T Message](ser serializer.Serializer[T]) Option[T] {
	return func(c *Config[T]) {
		c.serializer = ser
	}
}

func WithOnErrorFunc[T Message](onError connection.OnErrorFunc) Option[T] {
	return func(c *Config[T]) {
		c.onError = onError
	}
}

func WithContext[T Message](ctx context.Context) Option[T] {
	return func(c *Config[T]) {
		c.ctx = ctx
	}
}

func WithConnectionOptions[T Message](connectionOptions connection.Config) Option[T] {
	return func(c *Config[T]) {
		c.connectionOptions = connectionOptions
	}
}

func WithBufferedMessages[T Message](capacity int) Option[T] {
	return func(c *Config[T]) {
		c.messageBuffering = capacity
	}
}

func WithLogger[T Message](logger amqp.Logger) Option[T] {
	return func(c *Config[T]) {
		if logger != nil {
			c.logger = logger
		}
	}
}
