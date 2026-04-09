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

// WithRetryMessageCount sets the number of times a message will be retried before being dropped.
//
// WARNING: The retry mechanism used by retryHandler relies on modifying the X-Retry-Count
// header on the local amqp091.Delivery struct and nacking the message back to the broker
// with requeue=true. This does NOT work — RabbitMQ redelivers the original message
// unchanged, discarding any header modifications made by the consumer. As a result, the
// retry counter resets on every delivery and the message is requeued indefinitely.
func WithRetryMessageCount[T any](count uint32) Option[T] {
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
