package main

import (
	"context"
	"github.com/nano-interactive/go-amqp/connection"
	"github.com/rabbitmq/amqp091-go"
	"time"
)

func mustPass[T any](data T, err error) T {
	if err != nil {
		panic(err)
	}
	return data
}

func main() {
	conn := mustPass(connection.New(&connection.Config{
		Host:              "127.0.0.1",
		Port:              5672,
		User:              "guest",
		Password:          "guest",
		Vhost:             "/",
		ConnectionName:    "go-amqp",
		ReconnectRetry:    10,
		ReconnectInterval: 1 * time.Second,
		Channels:          1000,
	}))

	channel := mustPass(conn.RawConnection().Channel())

	err := channel.ExchangeDeclare("test", amqp091.ExchangeFanout, true, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	_ = mustPass(channel.QueueDeclare("test", true, false, false, false, nil))

	err = channel.QueueBind("test", "", "test", false, nil)

	if err != nil {
		panic(err)
	}

	message := []byte(`{"name": "test"}`)

	for i := 0; i < 10_000_000; i++ {
		err = channel.PublishWithContext(
			context.Background(),
			"test",
			"", false, false, amqp091.Publishing{
				ContentType: "application/json",
				Body:        message,
			})

		if err != nil {
			panic(err)
		}
	}
}
