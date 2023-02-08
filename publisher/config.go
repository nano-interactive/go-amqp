package publisher

import (
	"time"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/serializer"
)

type (
	Config[T Message] struct {
		serializer        serializer.Serializer[T]
		logger            amqp.Logger
		connectionTimeout time.Duration
		messageBuffering  int
	}

	Option[T Message] func(*Config[T])
)

func WithConnectionTimeout[T Message](timeout time.Duration) Option[T] {
	return func(c *Config[T]) {
		c.connectionTimeout = timeout
	}
}

func WithSerializer[T Message](ser serializer.Serializer[T]) Option[T] {
	return func(p *Config[T]) {
		p.serializer = ser
	}
}

func WithBufferedMessages[T Message](capacity int) Option[T] {
	return func(p *Config[T]) {
		p.messageBuffering = capacity
	}
}

func WithLogger[T Message](logger amqp.Logger) Option[T] {
	return func(c *Config[T]) {
		if logger != nil {
			c.logger = logger
		}
	}
}
