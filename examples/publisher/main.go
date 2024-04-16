package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/publisher"
)

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
		FrameSize:         8192,
	}

	ctx := context.Background()

	pub, err := publisher.New(
		"testing_publisher",
		publisher.WithContext[Message](ctx),
		publisher.WithConnectionOptions[Message](connConfig),
	)
	if err != nil {
		panic(err)
	}

	_, _ = fmt.Println("[INFO]: Publisher Created")

	message := Message{
		Name: "Nano Interactive",
	}

	errCount := 0

	for i := 1; i <= 10_000_000; i++ {
		if err = pub.Publish(ctx, message); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR]: Failed to publish message %d with error %v\n", i, err)
			errCount++
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if i%1_000_000 == 0 {
			_, _ = fmt.Printf("[INFO]: Message Publushed %d\n", i)
			time.Sleep(200 * time.Millisecond)
		}
	}

	_, _ = fmt.Printf("[INFO]: Total errors %d\n", errCount)

	if err = pub.Close(); err != nil {
		panic(err)
	}
}
