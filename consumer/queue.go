package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/nano-interactive/go-amqp"
	"github.com/rabbitmq/amqp091-go"

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
		watchDog   chan struct{}
		queueName  string
		wg         sync.WaitGroup
	}
)

func newQueue(
	base context.Context,
	queueName string,
	logger amqp.Logger,
	cfg *QueueConfig,
	conn connection.Connection,
	handler RawHandler,
	errorCb connection.OnErrorFunc,
) (*queue, error) {
	ctx, cancel := context.WithCancel(base)

	queue := &queue{
		queueName:  queueName,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
		cfg:        cfg,
		handler:    handler,
		watchDog:   make(chan struct{}, cfg.Workers),
	}

	conn.OnBeforeConnectionReady(func(ctx context.Context) error {
		return queue.closeHandlers(ctx)
	})

	conn.OnConnectionReady(func(ctx context.Context, connection *amqp091.Connection) error {
		newCtx, cancel := context.WithCancel(base)
		queue.ctx = newCtx
		queue.cancel = cancel
		return queue.Listen()
	})

	conn.OnError(errorCb)

	go watchdog(ctx, conn, &queue.wg, queue.watchDog, queue)

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

		go listener(q.ctx, &q.wg, channel, q.handler, q.logger, q.queueName, q.watchDog)
	}

	return nil
}

func (q *queue) closeHandlers(ctx context.Context) error {
	q.cancel()
	q.wg.Wait()

	return nil
}

func (q *queue) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = q.closeHandlers(ctx)
	if q.watchDog != nil {
		close(q.watchDog)
	}
	return q.connection.Close()
}
