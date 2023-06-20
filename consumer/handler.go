package consumer

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/rabbitmq/amqp091-go"
)

var ErrNoRetry = errors.New("no retry")

const retryHeader = "X-Retry-Count"

type (
	RawHandler interface {
		Handle(context.Context, *amqp091.Delivery) error
	}

	handler[T Message] struct {
		handler Handler[T]
	}

	retryHandler[T Message] struct {
		handler    Handler[T]
		retryCount uint32
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

func (h handler[T]) Handle(ctx context.Context, delivery *amqp091.Delivery) error {
	var body T

	switch delivery.ContentType {
	case "application/json":
		fallthrough
	default:
		if err := json.Unmarshal(delivery.Body, &body); err != nil {
			_ = delivery.Reject(false)
			return err
		}
	}

	if err := h.handler.Handle(ctx, body); err != nil {
		if errors.Is(err, ErrNoRetry) {
			_ = delivery.Reject(false)
			return err
		}

		if ackErr := delivery.Nack(false, false); ackErr != nil {
			return err
		}

		return err
	}

	if ackErr := delivery.Ack(false); ackErr != nil {
		return ackErr
	}

	return nil
}

func (h retryHandler[T]) Handle(ctx context.Context, delivery *amqp091.Delivery) error {
	var body T

	switch delivery.ContentType {
	case "application/json":
		fallthrough
	default:
		if err := json.Unmarshal(delivery.Body, &body); err != nil {
			_ = delivery.Reject(false)
			return err
		}
	}

	if err := h.handler.Handle(ctx, body); err != nil {
		_, ok := delivery.Headers[retryHeader]

		if !ok {
			delivery.Headers[retryHeader] = int64(h.retryCount)
		}

		if errors.Is(err, ErrNoRetry) {
			_ = delivery.Reject(false)
			return err
		}

		requeue := true
		valInt := delivery.Headers[retryHeader].(int64)

		if valInt <= 0 {
			requeue = false
		} else {
			delivery.Headers[retryHeader] = valInt - 1
		}

		if ackErr := delivery.Nack(false, requeue); ackErr != nil {
			return err
		}

		return err
	}

	if ackErr := delivery.Ack(false); ackErr != nil {
		return ackErr
	}

	return nil
}
