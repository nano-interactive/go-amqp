package consumer

import (
	"context"
	"github.com/nano-interactive/go-amqp/connection"
	"sync"
)

func watchdog(
	ctx context.Context,
	conn connection.Connection,
	wg *sync.WaitGroup,
	workerExit chan struct{},
	queue *queue,
) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-workerExit:
			channel, err := conn.RawConnection().Channel()
			if err != nil {
				queue.logger.Error("failed to create channel, trying again: %v", err)
				workerExit <- struct{}{}
				continue
			}

			if err = channel.Qos(queue.cfg.PrefetchCount, 0, false); err != nil {
				queue.logger.Error("failed to set prefetch count, trying again: %v", err)
				if !channel.IsClosed() {
					_ = channel.Close()
				}
				workerExit <- struct{}{}
				continue
			}

			go listener(ctx, wg, channel, queue.handler, queue.logger, queue.queueName, workerExit)
		}
	}
}
