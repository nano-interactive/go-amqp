package consumer

import (
	"context"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/serializer"
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
	ctx               context.Context
	logger            amqp.Logger
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
		c.queueConfig = cfg
	}
}

func WithLogger[T any](logger amqp.Logger) Option[T] {
	return func(c *Config[T]) {
		c.logger = logger
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
