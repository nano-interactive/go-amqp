package connection

import "context"

type (
	OnReconnectingFunc func(context.Context) error
	OnReconnectFunc    func(context.Context) error
	OnErrorFunc        func(error)
)

type Events struct {
	onReconnecting OnReconnectingFunc
	onReconnect    OnReconnectFunc
	onError        OnErrorFunc
}

func (c *Events) SetOnReconnect(onReconnect OnReconnectFunc) {
	c.onReconnect = onReconnect
}

func (c *Events) SetOnReconnecting(onReconnecting OnReconnectingFunc) {
	c.onReconnecting = onReconnecting
}

func (c *Events) SetOnError(onError OnErrorFunc) {
	c.onError = onError
}
