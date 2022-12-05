package amqp

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)



func listener[T Message](
	ctx context.Context,
	wg *sync.WaitGroup,
	channel *amqp091.Channel,
	config *QueueConfig,
	handler Handler[T],
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
	_ = channel.Qos(config.PrefetchCount, config.PrefetchSize, false)
	defer channel.Close()

	for {
		select {
		case delivery, more := <-dataStream:
			var body T

			switch delivery.ContentType {
			case "application/json":
				fallthrough
			default:
				if err := json.Unmarshal(delivery.Body, &body); err != nil {
					logger.Error("Failed to deserialize AMQP Message(%s): %v", queueName, err)
					if ackErr := delivery.Ack(false); ackErr != nil {
						logger.Error("Failed to ack AMQP Message(%s): %v", queueName, ackErr)
					}
					continue
				}
			}

			if err := handler.Handle(ctx, body); err != nil {
				if ackErr := delivery.Nack(false, true); ackErr != nil {
					logger.Error("Failed to nack AMQP Message(%s): %v", queueName, ackErr)
				}
				continue
			}

			if ackErr := delivery.Ack(false); ackErr != nil {
				logger.Error("Failed to ack AMQP Message(%s): %v", queueName, ackErr)
			}

			if !more {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func handleErrors[T Message](base context.Context, queue *queue[T], ch chan *amqp091.Error, connectionString string, amqpConfig amqp091.Config) {
	reconnect := func(amqpErr *amqp091.Error) error {
		var err error

		conn, err := amqp091.DialConfig(connectionString, amqpConfig)
		if err != nil {
			queue.logger.Error("Failed to reconnect to RabbitMQ: %v", err)
			return err
		}

		newCh := make(chan *amqp091.Error)
		conn.NotifyClose(newCh)
		go handleErrors(base, queue, newCh, connectionString, amqpConfig)

		if amqpErr.Code != amqp091.ConnectionForced {
			close(ch)
		}

		ctx, cancel := context.WithCancel(base)

		queue.connection = conn
		queue.ctx = ctx
		queue.cancel = cancel
		if err = queue.Listen(); err != nil {
			queue.logger.Error("Failed start listeners: %v", err)
			return err
		}

		return nil
	}

	for amqpErr := range ch {
		if shouldReconnect(amqpErr) {
			queue.Close()

			if err := reconnect(amqpErr); err != nil {
				for i := 0; i < queue.cfg.RetryCount-1; i++ {
					time.Sleep(queue.cfg.SleepInterval)
					if err := reconnect(amqpErr); err == nil {
						break
					}
				}
			}
		}
	}
}
