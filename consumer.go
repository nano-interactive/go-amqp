package amqp

import (
	"context"
	"github.com/nano-interactive/go-amqp/connection"
	"time"

	"go.uber.org/multierr"
)

type (
	Message interface {
		GetQueueName() string
	}

	Consumer struct {
		logger Logger
		ctx    context.Context
		queues []*queue
	}
)

func NewConsumer(ctx context.Context, logger ...Logger) *Consumer {
	var l Logger = &defaultLogger{}

	if len(logger) > 0 {
		l = logger[0]
	}

	return &Consumer{
		ctx:    ctx,
		logger: l,
		queues: make([]*queue, 0, 10),
	}
}

type defaultLogger struct{}

func (d defaultLogger) Error(msg string, args ...interface{}) {}

func AddListenerRaw(c *Consumer, h RawHandler, options ...Option) error {
	opt := Config{
		queueName: "",
		queueConfig: QueueConfig{
			PrefetchCount:        128,
			ConnectionNamePrefix: "",
			Workers:              1,
		},
		connectionConfig: connection.Config{
			Host:              "127.0.0.1",
			User:              "guest",
			Password:          "guest",
			Vhost:             "/",
			ConnectionName:    "go-amqp",
			Port:              5672,
			ReconnectRetry:    10,
			Channels:          100,
			ReconnectInterval: 5 * time.Second,
		},
		logger: &defaultLogger{},
	}

	for _, o := range options {
		o(&opt)
	}

	queue, err := newQueue(
		c.ctx,
		opt.queueName,
		opt.logger,
		&opt.queueConfig,
		&opt.connectionConfig,
		h,
	)
	if err != nil {
		return err
	}

	c.queues = append(c.queues, queue)
	return nil
}

func AddListenerRawFunc(c *Consumer, h RawHandlerFunc, options ...Option) error {
	return AddListenerRaw(c, h, options...)
}

func AddListener[T Message](c *Consumer, h Handler[T], options ...Option) error {
	var msg T
	return AddListenerRaw(c, &handler[T]{
		handler:   h,
		queueName: msg.GetQueueName(),
	}, options...)
}

func AddListenerFunc[T Message](c *Consumer, handler HandlerFunc[T], options ...Option) error {
	return AddListener[T](c, handler, options...)
}

func (c *Consumer) Start() error {
	var err error

	for _, q := range c.queues {
		if listenErr := q.Listen(); listenErr != nil {
			err = multierr.Append(err, listenErr)
		}
	}

	return err
}

func (c *Consumer) Close() error {
	var err error

	for _, q := range c.queues {
		if closeErr := q.Close(); closeErr != nil {
			err = multierr.Append(err, closeErr)
		}
	}

	return err
}
