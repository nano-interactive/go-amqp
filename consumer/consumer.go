package consumer

import (
	"context"
	"io"

	"go.uber.org/multierr"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
)

type (
	Message interface {
		GetQueueName() string
	}

	Sub[T Message] interface {
		io.Closer
		Start() error
	}

	Consumer[T Message] struct {
		logger amqp.Logger
		ctx    context.Context
		queues *queue
	}
)

func NewRaw[T Message](
	ctx context.Context,
	conn connection.Connection,
	h RawHandler,
	errorCb connection.OnErrorFunc,
	options ...Option,
) (*Consumer[T], error) {
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

	var msg T

	queue, err := newQueue(
		ctx,
		msg.GetQueueName(),
		opt.logger,
		&opt.queueConfig,
		conn,
		h,
		errorCb,
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

func NewRawFunc[T Message](ctx context.Context, conn connection.Connection, h RawHandlerFunc, errorCb connection.OnErrorFunc, options ...Option) (*Consumer[T], error) {
	return NewRaw[T](ctx, conn, h, errorCb, options...)
}

func New[T Message](ctx context.Context, conn connection.Connection, h HandlerFunc[T], errorCb connection.OnErrorFunc, options ...Option) (*Consumer[T], error) {
	var msg T

	return NewRaw[T](ctx, conn, &handler[T]{
		handler:   h,
		queueName: msg.GetQueueName(),
	}, errorCb, options...)
}

func NewHandler[T Message](ctx context.Context, conn connection.Connection, h Handler[T], errorCb connection.OnErrorFunc, options ...Option) (*Consumer[T], error) {
	var msg T

	return NewRaw[T](ctx, conn, &handler[T]{
		queueName: msg.GetQueueName(),
		handler:   h,
	}, errorCb, options...)
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
