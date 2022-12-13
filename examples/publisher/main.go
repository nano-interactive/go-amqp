package main

import (
	"context"
	"fmt"
	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/publisher"
	"time"
)

type logger struct{}

func (d logger) Error(msg string, args ...any) {
	fmt.Printf(msg+"\n", args...)
}

type Message struct {
	Name string `json:"name"`
}

func (m Message) GetExchangeName() string {
	return "test"
}

func (m Message) GetExchangeType() publisher.ExchangeType {
	return publisher.ExchangeTypeFanout
}

func main() {
	connConfig := &connection.Config{
		Host:              "127.0.0.1",
		Port:              5672,
		User:              "nano",
		Password:          "admin",
		Vhost:             "/",
		ConnectionName:    "go-amqp",
		ReconnectRetry:    10,
		ReconnectInterval: 1 * time.Second,
		Channels:          1000,
	}

	pool, err := connection.NewPool(10, connConfig)

	if err != nil {
		panic(err)
	}

	pub, err := publisher.New[Message](
		context.Background(),
		pool,
		publisher.WithLogger[Message](&logger{}),
	)

	if err != nil {
		panic(err)
	}

	message := Message{
		Name: "Dusan",
	}

	for i := 0; i < 100_000; i++ {
		err := pub.Publish(context.Background(), message)
		if err != nil {
			panic(err)
		}
	}
}
