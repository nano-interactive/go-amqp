package amqp

import (
	"context"
	"encoding/json"

	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/multierr"
)

type (
	RawHandler interface {
		Handle(context.Context, *amqp091.Delivery) error
	}

	handler[T Message] struct {
		queueName string
		handler   Handler[T]
	}

	Handler[T Message] interface {
		Handle(context.Context, T) error
	}

	HandlerFunc[T Message] func(context.Context, T) error
	RawHandlerFunc         func(context.Context, *amqp091.Delivery) error
)

func (h HandlerFunc[T]) Handle(ctx context.Context, body T) error {
	return h(ctx, body)
}

func (h RawHandlerFunc) Handle(ctx context.Context, body *amqp091.Delivery) error {
	return h(ctx, body)
}

func (h *handler[T]) Handle(ctx context.Context, delivery *amqp091.Delivery) error {
	var body T

	switch delivery.ContentType {
	case "application/json":
		fallthrough
	default:
		if err := json.Unmarshal(delivery.Body, &body); err != nil {
			var multiErr error

			multiErr = multierr.Append(multiErr, err)

			if ackErr := delivery.Ack(false); ackErr != nil {
				multiErr = multierr.Append(err, ackErr)
			}

			return multiErr
		}
	}

	if err := h.handler.Handle(ctx, body); err != nil {
		var multiErr error

		multiErr = multierr.Append(multiErr, err)

		if ackErr := delivery.Nack(false, true); ackErr != nil {
			multiErr = multierr.Append(err, ackErr)
		}

		return multiErr
	}

	if ackErr := delivery.Ack(false); ackErr != nil {
		return ackErr
	}

	return nil
}
