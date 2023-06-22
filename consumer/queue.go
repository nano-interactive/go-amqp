package consumer

import (
	"context"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
)

type (
	QueueConfig struct {
		Workers       int
		PrefetchCount int
	}

	queue struct {
		logger     amqp.Logger
		connection *connection.Connection
	}
)

func newQueue[T any](base context.Context, cfg Config[T], queueDeclare QueueDeclare, handler RawHandler) (*queue, error) {
	conn, err := connection.New(base, cfg.connectionOptions, connection.Events{
		OnConnectionReady: func(ctx context.Context, connection *amqp091.Connection) error {
			watchDog := make(chan int, cfg.queueConfig.Workers)
			for i := 0; i < cfg.queueConfig.Workers; i++ {
				watchDog <- i + 1
			}

			watcher, err := watchdog(ctx, connection, watchDog, cfg.onError, cfg, queueDeclare, handler)

			if err != nil {
				return err
			}

			go watcher()

			return nil
		},
		OnError: cfg.onError,
	})
	if err != nil {
		return nil, err
	}

	return &queue{
		logger:     cfg.logger,
		connection: conn,
	}, nil
}

func (q *queue) Close() error {
	return q.connection.Close()
}
