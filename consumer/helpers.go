package consumer

import (
	"context"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

func listener(
	ctx context.Context,
	wg *sync.WaitGroup,
	channel *amqp091.Channel,
	config *QueueConfig,
	handler RawHandler,
	logger Logger,
	queueName string,
) {
	defer wg.Done()

	dataStream, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		logger.Error("Failed to consume queue(%s): %v", queueName, err)
		return
	}

	// Extract to QueueConfig
	//if err = channel.Qos(config.PrefetchCount, 0, false); err != nil {
	//	logger.Error("Failed to set QoS: %v", err)
	//	return
	//}

	defer func(channel *amqp091.Channel) {
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
