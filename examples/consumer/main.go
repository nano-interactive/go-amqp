package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/consumer"
)

type Message struct {
	Name string `json:"name"`
}

var cnt atomic.Uint64

func handler(ctx context.Context, msg Message) error {
	defer cnt.Add(1)
	_, _ = fmt.Printf("[INFO] Message received: %d %s\n", cnt.Load(), msg.Name)
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	c, err := consumer.NewFunc(
		handler,
		connection.Config{
			Host:              "127.0.0.1",
			Port:              5672,
			User:              "guest",
			Password:          "guest",
			Vhost:             "/",
			ConnectionName:    "go-amqp-consumer",
			ReconnectRetry:    10,
			ReconnectInterval: 1 * time.Second,
			Channels:          1000,
			FrameSize:         8192,
		},
		consumer.QueueDeclare{
			ExchangeBindings: []consumer.ExchangeBinding{{ExchangeName: "testing_publisher"}},
			QueueName:        "testing_queue",
			Durable:          true,
		},
		consumer.WithOnMessageError[Message](func(ctx context.Context, d *amqp091.Delivery, err error) {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Message error: %s\n", err)
		}),
		consumer.WithQueueConfig[Message](consumer.QueueConfig{
			Workers:       1,
			PrefetchCount: 128,
		}),
	)
	if err != nil {
		panic(err)
	}

	go func() {
		if err := c.Start(ctx); err != nil {
			panic(err)
		}
	}()

	_, _ = fmt.Println("[INFO] Consumer started")
	<-sig
	cancel()
	_, _ = fmt.Println("[INFO] Signal Received")

	if err := c.Close(); err != nil {
		panic(err)
	}
}
