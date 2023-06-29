package consumer

import (
	"context"
	"sync"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp/v2/connection"
	"github.com/nano-interactive/go-amqp/v2/logging"
)

type (
	QueueConfig struct {
		Workers       int
		PrefetchCount int
	}

	queue struct {
		logger     logging.Logger
		connection *connection.Connection
	}
)

func newQueue[T any](base context.Context, cfg Config[T], queueDeclare QueueDeclare, handler RawHandler) (*queue, error) {
	ctx, cancel := context.WithCancel(base)

	var wg sync.WaitGroup

	conn, err := connection.New(base, cfg.connectionOptions, connection.Events{
		OnBeforeConnectionReady: func(_ context.Context) error {
			cancel()
			wg.Wait()
			ctx, cancel = context.WithCancel(base)
			return nil
		},

		OnConnectionReady: func(_ context.Context, connection *amqp091.Connection) error {
			watchDog := make(chan int, cfg.queueConfig.Workers)
			for i := 0; i < cfg.queueConfig.Workers; i++ {
				watchDog <- i + 1
			}

			wg.Add(1)
			watcher, err := watchdog(
				ctx,
				connection,
				watchDog,
				cfg.onError,
				cfg,
				queueDeclare,
				handler,
			)
			if err != nil {
				return err
			}

			go watcher(&wg)

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
