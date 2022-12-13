package consumer

import (
	"github.com/nano-interactive/go-amqp"
)

type Config struct {
	queueConfig QueueConfig
	logger      amqp.Logger
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
