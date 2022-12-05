package amqp

import "context"

type (
	Handler[T Message] interface {
		Handle(ctx context.Context, body T) error
	}

	HandlerFunc[T Message] func(ctx context.Context, body T) error
)

func (h HandlerFunc[T]) Handle(ctx context.Context, body T) error {
	return h(ctx, body)
}
