package consumer

import (
	"context"
	"github.com/nano-interactive/go-amqp"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

func listener(
	ctx context.Context,
	wg *sync.WaitGroup,
	channel *amqp091.Channel,
	handler RawHandler,
	logger amqp.Logger,
	queueName string,
	workerExit chan struct{},
) {
	defer wg.Done()

	dataStream, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		logger.Error("Failed to consume queue(%s): %v", queueName, err)
		return
	}

	defer func(channel *amqp091.Channel) {
		err := ctx.Err()

		if err != context.Canceled && err != context.DeadlineExceeded {
			workerExit <- struct{}{}
		}

		if !channel.IsClosed() {
			err := channel.Close()
			if err != nil {
				panic(err)
			}
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
