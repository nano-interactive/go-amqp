package consumer

import (
	"context"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
)

type Config struct {
	ctx               context.Context
	logger            amqp.Logger
	onError           connection.OnErrorFunc
	connectionOptions connection.Config
	queueConfig       QueueConfig
}

type Option func(*Config)

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
