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
		queues *queue
	}
)

func NewRaw[T Message](handler RawHandler, options ...Option) (Consumer[T], error) {
	var msg T

	cfg := Config{
		queueConfig: QueueConfig{
			PrefetchCount: 128,
			Workers:       1,
		},
		logger: amqp.EmptyLogger{},
		ctx:    context.Background(),
		onError: func(err error) {
			if errors.Is(err, connection.ErrRetriesExhausted) {
				panic(err)
			}

			fmt.Fprintf(os.Stderr, "[ERROR]: An error has occurred! %v\n", err)
		},
		connectionOptions: connection.DefaultConfig,
		queueName:         msg.GetQueueName(),
	}

	for _, o := range options {
		o(&cfg)
	}

	queue, err := newQueue(cfg.ctx, cfg, handler)
	if err != nil {
		return Consumer[T]{}, err
	}

	return Consumer[T]{
		queues: queue,
	}, nil
}

func NewRawFunc[T Message](h RawHandlerFunc, options ...Option) (Consumer[T], error) {
	return NewRaw[T](h, options...)
}

func New[T Message](h HandlerFunc[T], options ...Option) (Consumer[T], error) {
	var msg T

	privHandler := handler[T]{
		handler:   h,
		queueName: msg.GetQueueName(),
	}

	return NewRaw[T](privHandler, options...)
}

func NewHandler[T Message](h Handler[T], options ...Option) (Consumer[T], error) {
	var msg T

	privHandler := handler[T]{
		queueName: msg.GetQueueName(),
		handler:   h,
	}

	return NewRaw[T](privHandler, options...)
}

func (c *Consumer[T]) Close() error {
	if err := c.queues.Close(); err != nil {
		return err
	}

	return nil
}
