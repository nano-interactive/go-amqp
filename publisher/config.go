package publisher

import (
	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/serializer"
)

type (
	Config[T any] struct {
		serializer serializer.Serializer[T]
		onError    connection.OnErrorFunc
		exchange   ExchangeDeclare
		messageBuffering int
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

func WithBufferedMessages[T any](capacity int) Option[T] {
	return func(c *Config[T]) {
		c.messageBuffering = capacity
	}
}
