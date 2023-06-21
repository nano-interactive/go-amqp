package testing_utils

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/nano-interactive/go-amqp/connection"
	"github.com/rabbitmq/amqp091-go"
	"testing"
	"time"
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

func SetupAmqp(t testing.TB, cfg connection.Config, queueToExchangeBindings map[*string]*string) *amqp091.Channel {
	_, channel := GetAMQPConnection(t, cfg)
	t.Cleanup(func() {
		_ = channel.Close()
	})

	for queueName, exchangeName := range queueToExchangeBindings {
		*queueName = fmt.Sprintf("%s-%s", *queueName, randomString(16))
		*exchangeName = fmt.Sprintf("%s-%s", *exchangeName, randomString(16))

		err := channel.ExchangeDeclare(
			*exchangeName,
			amqp091.ExchangeFanout,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = channel.QueueDeclare(
			*queueName,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}

		_ = channel.QueueBind(
			*queueName,
			"",
			*exchangeName,
			false,
			nil,
		)

		t.Cleanup(func() {
			_, _ = channel.QueueDelete(*queueName, false, false, false)
			_ = channel.ExchangeDelete(*exchangeName, false, false)
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

func randomString(n int32) string {
	buffer := make([]byte, n)

	_, err := rand.Read(buffer)
	if err != nil {
		return ""
	}

	return base64.RawURLEncoding.EncodeToString(buffer)
}
