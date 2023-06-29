package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp/v2/connection"
)

func watchdog[T any](
	ctx context.Context,
	conn *amqp091.Connection,
	workerExit chan int,
	onError connection.OnErrorFunc,
	cfg Config[T],
	queueDeclare QueueDeclare,
	handler RawHandler,
) (func(*sync.WaitGroup), error) {
	var inner sync.WaitGroup

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	_, err = channel.QueueDeclare(
		queueDeclare.QueueName,
		queueDeclare.Durable,
		queueDeclare.AutoDelete,
		queueDeclare.Exclusive,
		queueDeclare.NoWait,
		nil,
	)

	if err != nil {
		return nil, err
	}

	for _, binding := range queueDeclare.ExchangeBindings {
		if err = channel.QueueBind(
			queueDeclare.QueueName,
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
		return nil, fmt.Errorf("failed to declare queue: %v", err)
	}

	return func(wg *sync.WaitGroup) {
		defer wg.Done()
		newCtx, cancel := context.WithCancel(ctx)
		defer close(workerExit)
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				inner.Wait()
				return
			case id := <-workerExit:
				inner.Add(1)

				l := newListener(
					id,
					queueDeclare.QueueName,
					cfg.queueConfig,
					conn,
					handler,
					cfg.onMessageError,
				)

				go func() {
					defer inner.Done()
					if cfg.onListenerStart != nil {
						cfg.onListenerStart(newCtx, id)
					}

					if cfg.onListenerExit != nil {
						defer cfg.onListenerExit(newCtx, id)
					}

					shouldRestart, err := l.Listen(newCtx, cfg.logger)
					if err != nil {
						onError(fmt.Errorf("failed to start listener: %v", err))
					}

					if shouldRestart {
						workerExit <- id
					}
				}()
			}
		}
	}, nil
}
