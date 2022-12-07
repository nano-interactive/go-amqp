package consumer

import (
	"context"
	"sync"

	"github.com/nano-interactive/go-amqp/connection"
)

type (
	Logger interface {
		Error(string, ...any)
	}
	QueueConfig struct {
		ConnectionNamePrefix string
		Workers              int
		PrefetchCount        int
	}

	queue struct {
		logger     Logger
		queueName  string
		connection connection.Connection
		ctx        context.Context
		cancel     context.CancelFunc
		cfg        *QueueConfig
		wg         *sync.WaitGroup
		handler    RawHandler
	}
)

func newQueue(
	base context.Context,
	queueName string,
	logger Logger,
	cfg *QueueConfig,
	connectionParams *connection.Config,
	handler RawHandler,
) (*queue, error) {
	ctx, cancel := context.WithCancel(base)

	wg := &sync.WaitGroup{}

	conn, err := connection.New(connectionParams)
	if err != nil {
		cancel()
		return nil, err
	}

	queue := &queue{
		queueName:  queueName,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		connection: conn,
		cfg:        cfg,
		wg:         wg,
		handler:    handler,
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
		err := queue.Listen()
		return err
	})

	conn.SetOnError(func(err error) {
		queue.logger.Error("Error: %v", err)
	})

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

	for i := 0; i < q.cfg.Workers; i++ {
		channel, err := conn.Channel()
		if err != nil {
			return err
		}

		if err = channel.Qos(q.cfg.PrefetchCount, 0, false); err != nil {
			return err
		}

		q.wg.Add(1)
		go listener(q.ctx, q.wg, channel, q.cfg, q.handler, q.logger, q.queueName)
	}

	return nil
}

func (q *queue) closeHandlers() {
	q.cancel()
	q.wg.Wait()
}

func (q *queue) Close() error {
	q.closeHandlers()
	return q.connection.Close()
}
