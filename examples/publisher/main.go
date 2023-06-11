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
	fmt.Printf("[ERROR]: "+msg+"\n", args...)
}

func (d logger) Info(msg string, args ...any) {
	fmt.Printf("[INFO]: " + msg+"\n", args...)
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
		User:              "guest",
		Password:          "guest",
		Vhost:             "/",
		ConnectionName:    "go-amqp-publisher",
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

	fmt.Print("[INFO]: Publisher Created")

	message := Message{
		Name: "Dusan",
	}

	for i := 0; i < 10_000_000; i++ {
		if err = pub.Publish(context.Background(), message); err != nil {
			panic(err)
		}

		if i % 100_000 == 0 {
			fmt.Printf("[INFO]: Message Publushed %d\n", i)
		}
	}

	err = pub.Close()
	if err != nil {
		panic(err)
	}
}
