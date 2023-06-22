package consumer

import (
	"context"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp"
)

type listener struct {
	handler        RawHandler
	conn           *amqp091.Connection
	onMessageError func(context.Context, *amqp091.Delivery, error)
	queueName      string
	cfg            QueueConfig
	id             int
}

func newListener(
	id int,
	queueName string,
	cfg QueueConfig,
	conn *amqp091.Connection,
	handler RawHandler,
	onMessageError func(context.Context, *amqp091.Delivery, error),
) *listener {
	if onMessageError == nil {
		panic("onMessageError is required")
	}

	return &listener{
		id:             id,
		conn:           conn,
		queueName:      queueName,
		cfg:            cfg,
		handler:        handler,
		onMessageError: onMessageError,
	}
}

func (l *listener) Listen(ctx context.Context, logger amqp.Logger) (shouldRestart bool, err error) {
	channel, err := l.conn.Channel()
	if err != nil {
		return true, err
	}

	if err = channel.Qos(l.cfg.PrefetchCount, 0, false); err != nil {
		return true, err
	}

	dataStream, err := channel.Consume(
		l.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Error("Failed to consume queue(%s): %v", l.queueName, err)
		return true, err
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
				return true, nil
			}

			if err := l.handler.Handle(ctx, &delivery); err != nil {
				l.onMessageError(ctx, &delivery, err)
				continue
			}
		case <-ctx.Done():
			return false, nil
		}
	}
}
