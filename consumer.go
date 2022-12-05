package amqp

import (
	"context"
	"sync"
)

type (
	Message interface {
		GetQueueName() string
	}

	Listener interface {
		Listen() error
		Close() error
	}

	Consumer struct {
		logger Logger
		ctx    context.Context
		queues []Listener
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
		queues: make([]Listener, 0, 10),
	}
}

type defaultLogger struct{}

func (d defaultLogger) Error(msg string, args ...interface{}) {}

func AddListener[T Message](c *Consumer, handler Handler[T], options ...Option) error {
	opt := Config{
		queueConfig: QueueConfig{
			PrefetchCount:        128,
			PrefetchSize:         128,
			ConnectionNamePrefix: "",
		},
		logger: &defaultLogger{},
	}

	for _, o := range options {
		o(&opt)
	}

	queue, err := newQueue(c.ctx, opt.logger, &opt.queueConfig, handler)
	if err != nil {
		return err
	}

	c.queues = append(c.queues, queue)
	return nil
}

func AddListenerFunc[T Message](c *Consumer, handler HandlerFunc[T], options ...Option) error {
	return AddListener[T](c, handler, options...)
}

func (c *Consumer) Start() error {
	for _, q := range c.queues {
		if err := q.Listen(); err != nil {
			return err
		}
	}

	return nil
}

func (c *Consumer) Close() error {
	wg := &sync.WaitGroup{}
	wg.Add(len(c.queues))

	// TODO: use errgroup

	for _, q := range c.queues {
		if err := q.Close(); err != nil {
			c.logger.Error("failed to close queue: %v", err)
			return err
		}
	}

	return nil
}
