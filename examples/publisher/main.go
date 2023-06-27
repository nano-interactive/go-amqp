package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nano-interactive/go-amqp/v2/connection"
	"github.com/nano-interactive/go-amqp/v2/publisher"
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
		"testing_publisher",
		publisher.WithContext[Message](ctx),
		publisher.WithConnectionOptions[Message](connConfig),
		publisher.WithLogger[Message](&logger{}),
	)
	if err != nil {
		panic(err)
	}

	fmt.Print("[INFO]: Publisher Created")

	message := Message{
		Name: "Nano Interactive",
	}

	errCount := 0

	for i := 1; i <= 10_000_000; i++ {
		if err = pub.Publish(ctx, message); err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR]: Failed to publish message %d with error %v\n", i, err)
			errCount++
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if i%1_000_000 == 0 {
			fmt.Printf("[INFO]: Message Publushed %d\n", i)
			time.Sleep(200 * time.Millisecond)
		}
	}

	fmt.Printf("[INFO]: Total errors %d\n", errCount)

	if err = pub.Close(); err != nil {
		panic(err)
	}
}
