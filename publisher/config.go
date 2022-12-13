package publisher

import (
	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/serializer"
	"time"
)

type (
	Config[T Message] struct {
		serializer        serializer.Serializer[T]
		logger            amqp.Logger
		connectionTimeout time.Duration
		publishCapacity   int
		publishers        int
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

func WithPublishCapacity[T Message](capacity int) Option[T] {
	return func(p *Config[T]) {
		p.publishCapacity = capacity
	}
}

func WithPublishers[T Message](publishers int) Option[T] {
	return func(p *Config[T]) {
		p.publishers = publishers
	}
}

func WithLogger[T Message](logger amqp.Logger) Option[T] {
	return func(c *Config[T]) {
		if logger != nil {
			c.logger = logger
		}
	}
}
