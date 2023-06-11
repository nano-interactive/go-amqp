package connection

import (
	"context"

	"github.com/rabbitmq/amqp091-go"
)

type (
	OnReconnectingFunc func(context.Context) error
	OnConnectionReady    func(context.Context, *amqp091.Connection) error
	OnErrorFunc        func(error)
)

type Events struct {
	onBeforeConnectionInit OnReconnectingFunc
	onConnectInit    OnConnectionReady
	onError        OnErrorFunc
}

func (c *Events) OnConnectionReady(onReconnect OnConnectionReady) {
	c.onConnectInit = onReconnect
}

func (c *Events) OnBeforeConnectionReady(onReconnecting OnReconnectingFunc) {
	c.onBeforeConnectionInit = onReconnecting
}

func (c *Events) OnError(onError OnErrorFunc) {
	c.onError = onError
}
