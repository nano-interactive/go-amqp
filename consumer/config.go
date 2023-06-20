package consumer

import (
	"context"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
)

type Config struct {
	ctx               context.Context
	logger            amqp.Logger
	onError           connection.OnErrorFunc
	onMessageError    func(context.Context, *amqp091.Delivery, error)
	onListenerStart   func(context.Context, int)
	onListenerExit    func(context.Context, int)
	queueConfig       QueueConfig
	connectionOptions connection.Config
	retryCount        uint32
}

type Option func(*Config)

func WithRetryMessageCountCount(count uint32) Option {
	return func(c *Config) {
		c.retryCount = count
	}
}

func WithOnListenerStart(onListenerStart func(context.Context, int)) Option {
	return func(c *Config) {
		c.onListenerStart = onListenerStart
	}
}

func WithOnListenerExit(onListenerExit func(context.Context, int)) Option {
	return func(c *Config) {
		c.onListenerExit = onListenerExit
	}
}

func WithOnMessageError(onMessageError func(context.Context, *amqp091.Delivery, error)) Option {
	return func(c *Config) {
		c.onMessageError = onMessageError
	}
}

func WithQueueConfig(cfg QueueConfig) Option {
	return func(c *Config) {
		c.queueConfig = cfg
	}
}

func WithLogger(logger amqp.Logger) Option {
	return func(c *Config) {
		c.logger = logger
	}
}

func WithOnErrorFunc(onError connection.OnErrorFunc) Option {
	return func(c *Config) {
		c.onError = onError
	}
}

func WithContext(ctx context.Context) Option {
	return func(c *Config) {
		c.ctx = ctx
	}
}

func WithConnectionOptions(connectionOptions connection.Config) Option {
	return func(c *Config) {
		c.connectionOptions = connectionOptions
	}
}
