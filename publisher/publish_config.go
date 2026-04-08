package publisher

type (
	PublishConfig struct {
		RoutingKey string
	}

	PublishOption func(*PublishConfig)
)

func WithRoutingKey(key string) PublishOption {
	return func(cfg *PublishConfig) {
		cfg.RoutingKey = key
	}
}
