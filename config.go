package amqp

type Config struct {
	queueConfig QueueConfig
	logger      Logger
}

type Option func(*Config)

func WithQueueConfig(cfg QueueConfig) Option {
	return func(c *Config) {
		if cfg.ConnectionParams.Host == "" {
			cfg.ConnectionParams.Host = "127.0.0.1"
		}

		if cfg.ConnectionParams.Port == 0 {
			cfg.ConnectionParams.Port = 5672
		}

		if cfg.ConnectionParams.User == "" {
			cfg.ConnectionParams.User = "guest"
		}

		if cfg.ConnectionParams.Password == "" {
			cfg.ConnectionParams.User = "guest"
		}

		if cfg.ConnectionParams.Vhost == "" {
			cfg.ConnectionParams.Vhost = ""
		}

		if cfg.Workers == 0 {
			cfg.Workers = 1
		}

		if cfg.PrefetchCount == 0 {
			cfg.PrefetchCount = 128
		}

		if cfg.PrefetchSize == 0 {
			cfg.PrefetchSize = 128
		}

		c.queueConfig = cfg
	}
}

func WithLogger(logger Logger) Option {
	return func(c *Config) {
		c.logger = logger
	}
}
