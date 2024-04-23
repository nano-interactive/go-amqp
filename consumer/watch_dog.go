package consumer

import (
	"context"

	"github.com/rabbitmq/amqp091-go"
)

func (c *Consumer[T, Q]) watchdogWatcher(ctx context.Context, conn *amqp091.Connection) {
	defer c.watcher.Release(1)

	queue := c.queueDeclare.QueueConfig()

	l := newListener(
		queue.QueueName,
		c.cfg.queueConfig,
		conn,
		c.handler,
		c.cfg.onMessageError,
	)

	if c.cfg.onListenerStart != nil {
		c.cfg.onListenerStart(ctx, 1)
	}

	if c.cfg.onListenerExit != nil {
		defer c.cfg.onListenerExit(ctx, 1)
	}

	if err := l.Listen(ctx); err != nil {
		c.cfg.onError(&ListenerStartFailedError{Inner: err})
	}
}

func (c *Consumer[T, Q]) watchdog(
	ctx context.Context,
	conn *amqp091.Connection,
) (func(), error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	queue := c.queueDeclare.QueueConfig()

	_, err = channel.QueueDeclare(
		queue.QueueName,
		queue.Durable,
		queue.AutoDelete,
		queue.Exclusive,
		queue.NoWait,
		nil,
	)
	if err != nil {
		return nil, err
	}

	for _, binding := range queue.ExchangeBindings {
		if err = channel.QueueBind(
			queue.QueueName,
			binding.RoutingKey,
			binding.ExchangeName,
			false,
			nil,
		); err != nil {
			return nil, err
		}
	}

	if !channel.IsClosed() {
		if err = channel.Close(); err != nil {
			return nil, err
		}
	} else {
		return nil, &QueueDeclarationError{Inner: err}
	}

	return func() {
		for {
			if err := c.watcher.Acquire(ctx, 1); err != nil {
				return
			}

			go c.watchdogWatcher(ctx, conn)
		}
	}, nil
}
