package consumer

import (
	"context"
	"fmt"

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
	conn, err := connection.New(base, c.cfg.connectionOptions, connection.Events{
		OnBeforeConnectionReady: func(ctx context.Context) error {
			defer c.watcher.Release(int64(c.cfg.queueConfig.Workers))
			// Here we wait for the workers to be released.
			return c.watcher.Acquire(ctx, int64(c.cfg.queueConfig.Workers))
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

	defer func(conn *connection.Connection) {
		err := conn.Close()
		if err != nil {
			fmt.Println(err.Error())
		}
	}(conn)

	<-base.Done()

	return nil
}
