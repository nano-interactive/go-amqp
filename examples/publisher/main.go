package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/publisher"
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

	conn, err := connection.New(connConfig)
	if err != nil {
		panic(err)
	}

	pub, err := publisher.New[Message](
		context.Background(),
		conn,
		publisher.WithLogger[Message](&logger{}),
		publisher.WithBufferedMessages[Message](1000),
	)
	if err != nil {
		panic(err)
	}

	message := Message{
		Name: "Dusan",
	}

	for i := 0; i < 1_000_000; i++ {
		err := pub.Publish(context.Background(), message)
		if err != nil {
			panic(err)
		}
	}

	err = pub.Close()
	if err != nil {
		panic(err)
	}
}
