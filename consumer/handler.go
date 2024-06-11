package consumer

import (
	"context"
	"errors"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp/v3/serializer"
)

var ErrNoRetry = errors.New("no retry")

const retryHeader = "X-Retry-Count"

type (
	RawHandler interface {
		Handle(context.Context, *amqp091.Delivery) error
	}

	handler[T Message] struct {
		serializer serializer.Serializer[T]
		handler    Handler[T]
	}

	retryHandler[T Message] struct {
		handler    handler[T]
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
	body, err := h.serializer.Unmarshal(delivery.Body)
	if err != nil {
		_ = delivery.Reject(false)
		return err
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

func (h retryHandler[T]) retry(err error, delivery *amqp091.Delivery) error {
	_, ok := delivery.Headers[retryHeader]

	if !ok {
		delivery.Headers = make(amqp091.Table)
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

func (h retryHandler[T]) Handle(ctx context.Context, delivery *amqp091.Delivery) error {
	body, err := h.handler.serializer.Unmarshal(delivery.Body)
	if err != nil {
		_ = delivery.Reject(false)
		return err
	}

	if err := h.handler.handler.Handle(ctx, body); err != nil {
		return h.retry(err, delivery)
	}

	if ackErr := delivery.Ack(false); ackErr != nil {
		return ackErr
	}

	return nil
}
