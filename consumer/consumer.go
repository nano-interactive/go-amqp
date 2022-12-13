package consumer

import (
	"context"

	"go.uber.org/multierr"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
)

type (
	Message interface {
		GetQueueName() string
	}

	Consumer[T Message] struct {
		logger amqp.Logger
		ctx    context.Context
		queues *queue
	}
)

func NewRaw[T Message](ctx context.Context, pool *connection.Pool, h RawHandler, options ...Option) (*Consumer[T], error) {
	var l amqp.Logger = &amqp.EmptyLogger{}

	opt := Config{
		queueConfig: QueueConfig{
			PrefetchCount:        128,
			ConnectionNamePrefix: "",
			Workers:              1,
		},
		logger: &amqp.EmptyLogger{},
	}

	for _, o := range options {
		o(&opt)
	}

	conn, err := pool.Get(ctx)

	if err != nil {
		return nil, err
	}

	var msg T

	queue, err := newQueue(
		ctx,
		msg.GetQueueName(),
		opt.logger,
		&opt.queueConfig,
		conn,
		h,
	)
	if err != nil {
		return nil, err
	}

	return &Consumer[T]{
		ctx:    ctx,
		logger: l,
		queues: queue,
	}, nil
}

func NewRawFunc[T Message](ctx context.Context, pool *connection.Pool, h RawHandlerFunc, options ...Option) (*Consumer[T], error) {
	return NewRaw[T](ctx, pool, h, options...)
}

func New[T Message](ctx context.Context, pool *connection.Pool, h HandlerFunc[T], options ...Option) (*Consumer[T], error) {
	var msg T

	return NewRaw[T](ctx, pool, &handler[T]{
		handler:   h,
		queueName: msg.GetQueueName(),
	}, options...)
}

func NewHandler[T Message](ctx context.Context, pool *connection.Pool, h Handler[T], options ...Option) (*Consumer[T], error) {
	var msg T

	return NewRaw[T](ctx, pool, &handler[T]{
		queueName: msg.GetQueueName(),
		handler:   h,
	}, options...)
}

func (c *Consumer[T]) Start() error {
	var err error

	if listenErr := c.queues.Listen(); listenErr != nil {
		err = multierr.Append(err, listenErr)
	}

	return err
}

func (c *Consumer[T]) Close() error {
	if err := c.queues.Close(); err != nil {
		return err
	}

	return nil
}
