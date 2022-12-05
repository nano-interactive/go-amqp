package connection

type Events struct {
	onReconnecting func() error
	onReconnect    func() error
	onError        func(err error)
}

func (c *Events) SetOnReconnect(onReconnect func() error) {
	c.onReconnect = onReconnect
}

func (c *Events) SetOnReconnecting(onReconnecting func() error) {
	c.onReconnecting = onReconnecting
}

func (c *Events) SetOnError(onError func(err error)) {
	c.onError = onError
}
