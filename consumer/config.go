package consumer

import (
	"context"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/serializer"
)

type ExchangeBinding struct {
	ExchangeName string
	RoutingKey   string
}

type QueueDeclare struct {
	QueueName        string
	ExchangeBindings []ExchangeBinding
	Durable          bool
	AutoDelete       bool
	Exclusive        bool
	NoWait           bool
}

type Config[T any] struct {
	serializer        serializer.Serializer[T]
	onError           connection.OnErrorFunc
	onMessageError    func(context.Context, *amqp091.Delivery, error)
	onListenerStart   func(context.Context, int)
	onListenerExit    func(context.Context, int)
	connectionOptions connection.Config
	queueConfig       QueueConfig
	retryCount        uint32
}

type Option[T any] func(*Config[T])

func WithMessageDeserializer[T any](serializer serializer.Serializer[T]) Option[T] {
	return func(c *Config[T]) {
		c.serializer = serializer
	}
}

func WithRetryMessageCountCount[T any](count uint32) Option[T] {
	return func(c *Config[T]) {
		c.retryCount = count
	}
}

func WithOnListenerStart[T any](onListenerStart func(context.Context, int)) Option[T] {
	return func(c *Config[T]) {
		c.onListenerStart = onListenerStart
	}
}

func WithOnListenerExit[T any](onListenerExit func(context.Context, int)) Option[T] {
	return func(c *Config[T]) {
		c.onListenerExit = onListenerExit
	}
}

func WithOnMessageError[T any](onMessageError func(context.Context, *amqp091.Delivery, error)) Option[T] {
	return func(c *Config[T]) {
		c.onMessageError = onMessageError
	}
}

func WithQueueConfig[T any](cfg QueueConfig) Option[T] {
	return func(c *Config[T]) {
		if cfg.Workers == 0 {
			cfg.Workers = 1
		}

		c.queueConfig = cfg
	}
}

func WithOnErrorFunc[T any](onError connection.OnErrorFunc) Option[T] {
	return func(c *Config[T]) {
		c.onError = onError
	}
}
