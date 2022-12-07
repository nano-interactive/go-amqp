package publisher

import "github.com/rabbitmq/amqp091-go"

type ExchangeType uint64

const (
	ExchangeTypeDirect ExchangeType = iota
	ExchangeTypeFanout
	ExchangeTypeTopic
	ExchangeTypeHeader
)

func (e ExchangeType) String() string {
	switch e {
	case ExchangeTypeDirect:
		return amqp091.ExchangeDirect
	case ExchangeTypeFanout:
		return amqp091.ExchangeFanout
	case ExchangeTypeTopic:
		return amqp091.ExchangeTopic
	default:
		panic("invalid exchange type")
	}
}
