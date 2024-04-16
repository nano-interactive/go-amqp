package consumer

import (
	"context"

	"github.com/rabbitmq/amqp091-go"
)

type listener struct {
	handler        RawHandler
	conn           *amqp091.Connection
	onMessageError func(context.Context, *amqp091.Delivery, error)
	queueName      string
	cfg            QueueConfig
}

func newListener(
	queueName string,
	cfg QueueConfig,
	conn *amqp091.Connection,
	handler RawHandler,
	onMessageError func(context.Context, *amqp091.Delivery, error),
) listener {
	return listener{
		conn:           conn,
		queueName:      queueName,
		cfg:            cfg,
		handler:        handler,
		onMessageError: onMessageError,
	}
}

func (l listener) Listen(ctx context.Context) error {
	channel, err := l.conn.Channel()
	if err != nil {
		return err
	}

	if err = channel.Qos(l.cfg.PrefetchCount, 0, false); err != nil {
		return err
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
				return nil
			}

			if err := l.handler.Handle(ctx, &delivery); err != nil {
				l.onMessageError(ctx, &delivery, err)
				continue
			}
		case <-ctx.Done():
			return nil
		}
	}
}
