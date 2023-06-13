package consumer

import (
	"context"
	"sync"

	"github.com/nano-interactive/go-amqp"
	"github.com/rabbitmq/amqp091-go"
)

type listener struct {
	handler        RawHandler
	wg             *sync.WaitGroup
	conn           *amqp091.Connection
	workerExit     chan<- int
	onMessageError func(context.Context, *amqp091.Delivery, error)
	cfg            QueueConfig
	id             int
	shouldRestart  bool
}

func newListener(
	id int,
	wg *sync.WaitGroup,
	cfg QueueConfig,
	conn *amqp091.Connection,
	handler RawHandler,
	workerExit chan<- int,
	onMessageError func(context.Context, *amqp091.Delivery, error),
) *listener {
	return &listener{
		id:             id,
		wg:             wg,
		cfg:            cfg,
		conn:           conn,
		handler:        handler,
		workerExit:     workerExit,
		shouldRestart:  false,
		onMessageError: onMessageError,
	}
}

func (l *listener) Close() error {
	if l.shouldRestart {
		l.workerExit <- l.id
	}
	return nil
}

func (l *listener) Listen(ctx context.Context, logger amqp.Logger) error {
	defer l.wg.Done()

	channel, err := l.conn.Channel()
	if err != nil {
		l.shouldRestart = true
		return err
	}

	if err = channel.Qos(l.cfg.PrefetchCount, 0, false); err != nil {
		l.shouldRestart = true
		return err
	}

	dataStream, err := channel.Consume(l.cfg.QueueName, "", false, false, false, false, nil)
	if err != nil {
		logger.Error("Failed to consume queue(%s): %v", l.cfg.QueueName, err)
		l.shouldRestart = true
		return err
	}

	defer func(channel *amqp091.Channel) {
		if !channel.IsClosed() {
			_ = channel.Close()
		}
	}(channel)

	for {
		select {
		case delivery, more := <-dataStream:
			if !more {
				l.shouldRestart = true
				return nil
			}

			if err := l.handler.Handle(ctx, &delivery); err != nil {
				if l.onMessageError != nil {
					l.onMessageError(ctx, &delivery, err)
				}
				continue
			}
		case <-ctx.Done():
			l.shouldRestart = false

			return nil
		}
	}
}
