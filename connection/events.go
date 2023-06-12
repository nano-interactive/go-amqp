package connection

import (
	"context"

	"github.com/rabbitmq/amqp091-go"
)

type (
	OnReconnectingFunc func(context.Context) error
	OnConnectionReady  func(context.Context, *amqp091.Connection) error
	OnErrorFunc        func(error)
)
