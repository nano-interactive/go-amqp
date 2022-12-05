package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
)

type Message struct {
	Name string `json:"name"`
}

func (m Message) GetQueueName() string {
	return "test"
}

func handler(ctx context.Context, msg Message) error {
	fmt.Println("Message received:", msg.Name)
	return nil
}

type logger struct{}

func (l *logger) Error(msg string, args ...interface{}) {
	fmt.Printf(msg+"\n", args...)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	consumer := amqp.NewConsumer(ctx)

	err := amqp.AddListenerFunc(consumer, handler,
		amqp.WithQueueName("test"),
		amqp.WithLogger(&logger{}),
		amqp.WithConnectionConfig(connection.Config{
			Host:              "127.0.0.1",
			Port:              5672,
			User:              "guest",
			Password:          "guest",
			Vhost:             "/",
			ConnectionName:    "go-amqp",
			ReconnectRetry:    10,
			ReconnectInterval: 1 * time.Second,
			Channels:          1000,
		}),
		amqp.WithQueueConfig(amqp.QueueConfig{
			ConnectionNamePrefix: "go-amqp",
			Workers:              1,
			PrefetchCount:        128,
		}),
	)
	if err != nil {
		panic(err)
	}

	if err := consumer.Start(); err != nil {
		panic(err)
	}

	<-sig
	if err := consumer.Close(); err != nil {
		panic(err)
	}
}
