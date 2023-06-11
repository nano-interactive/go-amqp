package consumer

import (
	"context"
	"sync"

	"github.com/nano-interactive/go-amqp"

	"github.com/nano-interactive/go-amqp/connection"
)

type (
	QueueConfig struct {
		ConnectionNamePrefix string
		Workers              int
		PrefetchCount        int
	}

	queue struct {
		logger     amqp.Logger
		connection connection.Connection
		ctx        context.Context
		handler    RawHandler
		cancel     context.CancelFunc
		cfg        *QueueConfig
		wg         *sync.WaitGroup
		watchDog   chan struct{}
		queueName  string
	}
)

func newQueue(
	base context.Context,
	queueName string,
	logger amqp.Logger,
	cfg *QueueConfig,
	conn connection.Connection,
	handler RawHandler,
) (*queue, error) {
	ctx, cancel := context.WithCancel(base)

	wg := &sync.WaitGroup{}

	queue := &queue{
		queueName:  queueName,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		connection: conn,
		cfg:        cfg,
		wg:         wg,
		handler:    handler,
		watchDog:   make(chan struct{}, cfg.Workers),
	}

	conn.SetOnReconnecting(func() error {
		queue.closeHandlers()
		return nil
	})

	conn.SetOnReconnect(func() error {
		ctx, cancel = context.WithCancel(base)
		// Here we know -> this is the only thread modifying the context and cancel
		queue.ctx = ctx
		queue.cancel = cancel
		return queue.Listen()
	})

	conn.SetOnError(func(err error) {
		queue.logger.Error("Error: %v", err)
	})

	go watchdog(ctx, conn, wg, queue.watchDog, queue)

	return queue, nil
}

func (q *queue) Listen() error {
	conn := q.connection.RawConnection()
	amqpChannel, err := conn.Channel()
	if err != nil {
		return err
	}

	if _, err = amqpChannel.QueueDeclare(q.queueName, true, false, false, false, nil); err != nil {
		return err
	}

	if err = amqpChannel.Close(); err != nil {
		return err
	}

	q.wg.Add(q.cfg.Workers + 1)
	for i := 0; i < q.cfg.Workers; i++ {
		channel, err := conn.Channel()
		if err != nil {
			return err
		}

		if err = channel.Qos(q.cfg.PrefetchCount, 0, false); err != nil {
			return err
		}

		go listener(q.ctx, q.wg, channel, q.handler, q.logger, q.queueName, q.watchDog)
	}

	return nil
}

func (q *queue) closeHandlers() {
	q.cancel()
	q.wg.Wait()
}

func (q *queue) Close() error {
	q.closeHandlers()
	if q.watchDog != nil {
		close(q.watchDog)
	}
	return q.connection.Close()
}
