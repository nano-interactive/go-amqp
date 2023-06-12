package consumer

import (
	"context"
	"sync"

	"github.com/nano-interactive/go-amqp"
	"github.com/rabbitmq/amqp091-go"
)

func listener(
	ctx context.Context,
	wg *sync.WaitGroup,
	queueName string,
	cfg QueueConfig,
	logger amqp.Logger,
	conn *amqp091.Connection,
	handler RawHandler,
	workerExit chan<- struct{},
) {
	defer wg.Done()

	channel, err := conn.Channel()
	if err != nil {
		workerExit <- struct{}{}
		return
	}

	if err = channel.Qos(cfg.PrefetchCount, 0, false); err != nil {
		workerExit <- struct{}{}
		return
	}

	dataStream, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		logger.Error("Failed to consume queue(%s): %v", queueName, err)
		workerExit <- struct{}{}
		return
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
				return
			}

			if err := handler.Handle(ctx, &delivery); err != nil {
				logger.Error("Failed to handle message: %v", err)
				continue
			}
		case <-ctx.Done():
			return
		}
	}
}
