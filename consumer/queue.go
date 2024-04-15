package consumer

import (
	"context"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/rabbitmq/amqp091-go"
)

type (
	QueueConfig struct {
		Workers       int
		PrefetchCount int
	}
)

func (c *Consumer[T]) Start(base context.Context) error {
	_, cancel := context.WithCancel(base)

	conn, err := connection.New(base, c.cfg.connectionOptions, connection.Events{
		OnBeforeConnectionReady: func(ctx context.Context) error {
			cancel()
			_, cancel = context.WithCancel(ctx)
			return c.watcher.Acquire(base, int64(c.cfg.queueConfig.Workers))
		},

		OnConnectionReady: func(ctx context.Context, connection *amqp091.Connection) error {
			fn, err := c.watchdog(ctx, connection)

			if err != nil {
				return err
			}

			go fn()

			return nil
		},
		OnError: c.cfg.onError,
	})

	if err != nil {
		return err
	}

	defer conn.Close()

	return nil
}
