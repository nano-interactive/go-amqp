package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
)

type (
	Message interface {
		GetQueueName() string
	}

	Sub[T Message] interface {
		io.Closer
	}

	Consumer[T Message] struct {
		logger amqp.Logger
		queues *queue
	}
)

func NewRaw[T Message](
	h RawHandler,
	options ...Option,
) (*Consumer[T], error) {
	var l amqp.Logger = &amqp.EmptyLogger{}

	opt := Config{
		queueConfig: QueueConfig{
			PrefetchCount: 128,
			Workers:       1,
		},
		logger: &amqp.EmptyLogger{},
		ctx:    context.Background(),
		onError: func(err error) {
			if errors.Is(err, connection.ErrRetriesExhausted) {
				panic(err)
			}

			fmt.Fprintf(os.Stderr, "[ERROR]: An error has occurred! %v\n", err)
		},
		connectionOptions: connection.DefaultConfig,
	}

	for _, o := range options {
		o(&opt)
	}

	var msg T

	queue, err := newQueue(
		opt.ctx,
		msg.GetQueueName(),
		opt,
		h,
	)
	if err != nil {
		return nil, err
	}

	return &Consumer[T]{
		logger: l,
		queues: queue,
	}, nil
}

func NewRawFunc[T Message](h RawHandlerFunc, options ...Option) (*Consumer[T], error) {
	return NewRaw[T](h, options...)
}

func New[T Message](h HandlerFunc[T], options ...Option) (*Consumer[T], error) {
	var msg T

	return NewRaw[T](&handler[T]{
		handler:   h,
		queueName: msg.GetQueueName(),
	}, options...)
}

func NewHandler[T Message](h Handler[T], options ...Option) (*Consumer[T], error) {
	var msg T

	return NewRaw[T](&handler[T]{
		queueName: msg.GetQueueName(),
		handler:   h,
	}, options...)
}

func (c *Consumer[T]) Close() error {
	if err := c.queues.Close(); err != nil {
		return err
	}

	return nil
}
