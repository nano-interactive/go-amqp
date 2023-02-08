package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/consumer"
)

type Message struct {
	Name string `json:"name"`
}

func (m Message) GetQueueName() string {
	return "testing_queue"
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

	pool, err := connection.New(connConfig)
	if err != nil {
		panic(err)
	}

	c, err := consumer.New(ctx, pool, handler,
		consumer.WithLogger(&logger{}),
		consumer.WithQueueConfig(consumer.QueueConfig{
			ConnectionNamePrefix: "go-amqp",
			Workers:              1,
			PrefetchCount:        128,
		}),
	)
	if err != nil {
		panic(err)
	}

	if err := c.Start(); err != nil {
		panic(err)
	}

	<-sig
	if err := c.Close(); err != nil {
		panic(err)
	}

	if err := pool.Close(); err != nil {
		panic(err)
	}
}
