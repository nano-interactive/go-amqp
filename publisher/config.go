package publisher

import (
	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/serializer"
)

type (
	PublisherConfig[T Message] struct {
		connectionConfig connection.Config
		serializer       serializer.Serializer[T]
		exchangeName     string
		exchangeType     ExchangeType
	}

	PublisherOption[T Message] func(*PublisherConfig[T])
)

func WithExchangeName[T Message](exchangeName string) PublisherOption[T] {
	return func(p *PublisherConfig[T]) {
		p.exchangeName = exchangeName
	}
}

func WithExchangeType[T Message](exchangeType ExchangeType) PublisherOption[T] {
	return func(p *PublisherConfig[T]) {
		p.exchangeType = exchangeType
	}
}

func WithConnectionConfig[T Message](cfg connection.Config) PublisherOption[T] {
	return func(p *PublisherConfig[T]) {
		p.connectionConfig = cfg
	}
}

func WithSerializer[T Message](ser serializer.Serializer[T]) PublisherOption[T] {
	return func(p *PublisherConfig[T]) {
		p.serializer = ser
	}
}
