package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/consumer"
	"github.com/rabbitmq/amqp091-go"
)

type Message struct {
	Name string `json:"name"`
}

var cnt atomic.Uint64

func handler(ctx context.Context, msg Message) error {
	defer cnt.Add(1)
	fmt.Printf("[INFO] Message received: %d %s\n", cnt.Load(), msg.Name)
	return nil
}

type logger struct{}

func (d logger) Error(msg string, args ...any) {
	fmt.Printf("[ERROR]: "+msg+"\n", args...)
}

func (d logger) Info(msg string, args ...any) {
	fmt.Printf("[INFO]: "+msg+"\n", args...)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	c, err := consumer.NewFunc(handler,
		consumer.QueueDeclare{
			ExchangeBindings: []consumer.ExchangeBinding{{ExchangeName: "testing_publisher"}},
			QueueName:        "testing_queue",
			Durable:          true,
		},
		consumer.WithOnMessageError[Message](func(ctx context.Context, d *amqp091.Delivery, err error) {
			fmt.Fprintf(os.Stderr, "[ERROR] Message error: %s\n", err)
		}),
		consumer.WithContext[Message](ctx),
		consumer.WithConnectionOptions[Message](connection.Config{
			Host:              "127.0.0.1",
			Port:              5672,
			User:              "guest",
			Password:          "guest",
			Vhost:             "/",
			ConnectionName:    "go-amqp-consumer",
			ReconnectRetry:    10,
			ReconnectInterval: 1 * time.Second,
			Channels:          1000,
		}),
		consumer.WithLogger[Message](logger{}),
		consumer.WithQueueConfig[Message](consumer.QueueConfig{
			Workers:       1,
			PrefetchCount: 128,
		}),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("[INFO] Consumer started")
	<-sig
	cancel()
	fmt.Println("[INFO] Signal Recieved")

	if err := c.Close(); err != nil {
		panic(err)
	}
}
