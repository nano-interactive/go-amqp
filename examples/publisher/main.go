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
	fmt.Printf("[INFO]: "+msg+"\n", args...)
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

func (m Message) GetRoutingKey() string {
	return ""
}

func main() {
	connConfig := connection.Config{
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

	ctx := context.Background()

	pub, err := publisher.New[Message](
		publisher.WithContext[Message](ctx),
		publisher.WithConnectionOptions[Message](connConfig),
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

	errCount := 0

	for i := 0; i < 10_000_00; i++ {
		if err = pub.Publish(ctx, message); err != nil {
			fmt.Printf("[ERROR]: Failed to publish message %d with error %v\n", i, err)
			errCount++
			continue
		}

		if i%100_000 == 0 {
			fmt.Printf("[INFO]: Message Publushed %d\n", i)
		}
	}

	fmt.Printf("[INFO]: Total errors %d\n", errCount)

	if err = pub.Close(); err != nil {
		panic(err)
	}
}
