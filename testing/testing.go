package testing

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/consumer"
	"github.com/nano-interactive/go-amqp/publisher"
)

func GetAMQPConnection(t testing.TB, cfg connection.Config) (*amqp091.Connection, *amqp091.Channel) {
	t.Helper()

	connectionURI := fmt.Sprintf(
		"amqp://%s:%s@%s:%d",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
	)

	properties := amqp091.NewConnectionProperties()
	properties.SetClientConnectionName("testing_connection_name")

	config := amqp091.Config{
		Vhost:      cfg.Vhost,
		ChannelMax: 1000,
		Properties: properties,
		Dial:       amqp091.DefaultDial(10 * time.Second),
	}

	conn, err := amqp091.DialConfig(connectionURI, config)
	if err != nil {
		t.Fatal(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err = ch.Close(); err != nil {
			t.Logf("error closing channel: %v", err)
			return
		}

		if err = conn.Close(); err != nil {
			t.Logf("error closing connection: %v", err)
			return
		}
	})

	return conn, ch
}

type PublisherConfig struct {
	Name   string
	Config publisher.ExchangeDeclare
}

type ConsumerConfig struct {
	Name   string
	Config consumer.QueueDeclare
}

func SetupAmqp(t testing.TB, cfg connection.Config, queueToExchangeBindings map[*ConsumerConfig]*PublisherConfig) *amqp091.Channel {
	t.Helper()
	_, channel := GetAMQPConnection(t, cfg)

	for queue, exchange := range queueToExchangeBindings {
		if exchange == nil {
			panic("exchange config is nil")
		}

		if queue == nil {
			panic("queue config is nil")
		}

		queue.Name = fmt.Sprintf("%s-%s", queue.Name, t.Name())
		exchange.Name = fmt.Sprintf("%s-%s", exchange.Name, t.Name())

		kind := amqp091.ExchangeFanout

		if exchange.Config.Type.String() != "" {
			kind = exchange.Config.Type.String()
		}

		durable := true
		if !exchange.Config.Durable {
			durable = false
		}

		autoDelete := true

		if !exchange.Config.AutoDelete {
			autoDelete = false
		}

		args := make(amqp091.Table)

		if exchange.Config.Args != nil {
			args = exchange.Config.Args
		}

		err := channel.ExchangeDeclare(
			exchange.Name,
			kind,
			durable,
			autoDelete,
			exchange.Config.Internal,
			exchange.Config.NoWait,
			args,
		)
		if err != nil {
			t.Fatal(err)
		}

		durable = true
		if !queue.Config.Durable {
			durable = false
		}

		autoDelete = true
		if !queue.Config.AutoDelete {
			autoDelete = false
		}

		_, err = channel.QueueDeclare(
			queue.Name,
			durable,
			autoDelete,
			queue.Config.Exclusive,
			queue.Config.NoWait,
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}

		_ = channel.QueueBind(
			queue.Name,
			"",
			exchange.Name,
			false,
			nil,
		)

		t.Cleanup(func() {
			_, _ = channel.QueueDelete(queue.Name, false, false, false)
			_ = channel.ExchangeDelete(exchange.Name, false, false)
		})
	}

	return channel
}


func ConsumeAMQPMessages[T any](
	t testing.TB,
	ctx context.Context,
	channel *amqp091.Channel,
	queueName string,
) []T {
	t.Helper()
	messages := make([]T, 0, 10)

	ch, err := channel.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	for {
		select {
		case d, more := <-ch:
			if !more {
				return messages
			}

			if err = d.Ack(false); err != nil {
				t.Fatal(err)
			}

			var data T

			if err = json.Unmarshal(d.Body, &data); err != nil {
				t.Fatal(err)
			}

			messages = append(messages, data)
		case <-ctx.Done():
			return messages
		}
	}
}

func PublishAMQPMessage[T any](
	t testing.TB,
	ctx context.Context,
	channel *amqp091.Channel,
	exchange string,
	msg T,
) {
	t.Helper()
	message, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	err = channel.PublishWithContext(
		ctx,
		exchange,
		"",
		false,
		false,
		amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent,
			Timestamp:    time.Now(),
			Body:         message,
		},
	)

	if err != nil {
		t.Fatal(err)
	}
}