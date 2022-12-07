package consumer

import (
	"github.com/nano-interactive/go-amqp/connection"
)

type Config struct {
	queueName        string
	queueConfig      QueueConfig
	connectionConfig connection.Config
	logger           Logger
}

type Option func(*Config)

func WithQueueName(name string) Option {
	return func(c *Config) {
		c.queueName = name
	}
}

func WithConnectionConfig(cfg connection.Config) Option {
	return func(c *Config) {
		c.connectionConfig = cfg
	}
}

func WithQueueConfig(cfg QueueConfig) Option {
	return func(c *Config) {
		c.queueConfig = cfg
	}
}

func WithLogger(logger Logger) Option {
	return func(c *Config) {
		c.logger = logger
	}
}
