package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/nano-interactive/go-amqp/connection"
	"github.com/rabbitmq/amqp091-go"
)

func watchdog(
	ctx context.Context,
	conn *amqp091.Connection,
	workerExit chan struct{},
	onError connection.OnErrorFunc,
	cfg Config,
	handler RawHandler,
) {
	var wg sync.WaitGroup
	defer close(workerExit)

	ctx, cancel := context.WithCancel(ctx)

	for {
		select {
		case <-ctx.Done():
			cancel()
			wg.Wait()
			return
		case <-workerExit:
			channel, err := conn.Channel()
			if err != nil {
				onError(fmt.Errorf("failed to create channel, trying again: %v", err))
				workerExit <- struct{}{}
				continue
			}

			if err = channel.Qos(cfg.queueConfig.PrefetchCount, 0, false); err != nil {
				onError(fmt.Errorf("failed to set prefetch count, trying again: %v", err))

				if !channel.IsClosed() {
					_ = channel.Close()
				}
				workerExit <- struct{}{}
				continue
			}

			if err = channel.Close(); err != nil {
				onError(fmt.Errorf("failed to close channel, trying again: %v", err))
				workerExit <- struct{}{}
				continue
			}

			wg.Add(1)
			go listener(ctx, &wg, cfg.queueName, cfg.queueConfig, cfg.logger, conn, handler, workerExit)
		}
	}
}
