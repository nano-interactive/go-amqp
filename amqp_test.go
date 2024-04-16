package amqp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/consumer"
	"github.com/nano-interactive/go-amqp/v3/publisher"
)

type Message struct {
	Name string `json:"name"`
}

var cnt atomic.Uint64

func handler(_ context.Context, msg Message) error {
	defer cnt.Add(1)
	//nolint:forbidigo
	_, _ = fmt.Printf("[INFO] Message received: %d %s\n", cnt.Load(), msg.Name)
	return nil
}

func ExampleConsumer() {
	c, err := consumer.NewFunc(handler,
		consumer.QueueDeclare{QueueName: "testing_queue"},
		consumer.WithOnMessageError[Message](func(_ context.Context, _ *amqp091.Delivery, err error) {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Message error: %s\n", err)
		}),
		consumer.WithConnectionOptions[Message](connection.Config{
			Host:           "127.0.0.1",
			User:           "guest",
			Password:       "guest",
			ConnectionName: "go-amqp-consumer",
		}),
	)
	if err != nil {
		panic(err)
	}

	go func() {
		if err := c.Start(context.Background()); err != nil {
			panic(err)
		}
	}()

	fmt.Println("[INFO] Consumer started")
	time.Sleep(100 * time.Second)

	if err := c.Close(); err != nil {
		panic(err)
	}
}

type MyHandler struct{}

func (h MyHandler) Handle(_ context.Context, msg Message) error {
	defer cnt.Add(1)
	//nolint:forbidigo
	_, _ = fmt.Printf("[INFO] Message received: %d %s\n", cnt.Load(), msg.Name)
	return nil
}

func ExampleConsumerWithHandler() {
	c, err := consumer.New[Message](MyHandler{},
		consumer.QueueDeclare{QueueName: "testing_queue"},
		consumer.WithOnMessageError[Message](func(_ context.Context, _ *amqp091.Delivery, err error) {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Message error: %s\n", err)
		}),
		consumer.WithConnectionOptions[Message](connection.Config{
			Host:           "127.0.0.1",
			User:           "guest",
			Password:       "guest",
			ConnectionName: "go-amqp-consumer",
		}),
	)
	if err != nil {
		panic(err)
	}

	go func() {
		if err := c.Start(context.Background()); err != nil {
			panic(err)
		}
	}()

	fmt.Println("[INFO] Consumer started")
	time.Sleep(100 * time.Second)

	if err := c.Close(); err != nil {
		panic(err)
	}
}

func ExampleConsumerWithSignal() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	c, err := consumer.NewFunc(handler,
		consumer.QueueDeclare{QueueName: "testing_queue"},
		consumer.WithOnMessageError[Message](func(_ context.Context, _ *amqp091.Delivery, err error) {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Message error: %s\n", err)
		}),
		consumer.WithContext[Message](ctx),
		consumer.WithConnectionOptions[Message](connection.Config{
			Host:           "127.0.0.1",
			User:           "guest",
			Password:       "guest",
			ConnectionName: "go-amqp-consumer",
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

	fmt.Println("[INFO] Consumer started")
	<-sig
	cancel()
	fmt.Println("[INFO] Signal Received")

	if err := c.Close(); err != nil {
		panic(err)
	}
}

type MyRawHandler struct{}

func (h MyRawHandler) Handle(_ context.Context, d *amqp091.Delivery) error {
	defer cnt.Add(1)
	var msg Message

	_ = json.Unmarshal(d.Body, &msg)

	//nolint:forbidigo
	fmt.Printf("[INFO] Message received: %d %s\n", cnt.Load(), msg.Name)

	return d.Ack(false)
}

func Example_ConsumerWithRawHandler() {
	c, err := consumer.NewRaw(MyRawHandler{},
		consumer.QueueDeclare{QueueName: "testing_queue"},
		consumer.WithOnMessageError[Message](func(_ context.Context, _ *amqp091.Delivery, err error) {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Message error: %s\n", err)
		}),
		consumer.WithConnectionOptions[Message](connection.Config{
			Host:           "127.0.0.1",
			User:           "guest",
			Password:       "guest",
			ConnectionName: "go-amqp-consumer",
		}),
	)
	if err != nil {
		panic(err)
	}

	go func() {
		if err := c.Start(context.Background()); err != nil {
			panic(err)
		}
	}()

	fmt.Println("[INFO] Consumer started")
	time.Sleep(100 * time.Second)

	if err := c.Close(); err != nil {
		panic(err)
	}
}

func ExamplePublisher() {
	pub, err := publisher.New[Message](
		"testing_publisher",
		publisher.WithConnectionOptions[Message](connection.Config{
			Host:           "127.0.0.1",
			User:           "guest",
			Password:       "guest",
			ConnectionName: "go-amqp-publisher",
		}),
	)
	if err != nil {
		panic(err)
	}

	message := Message{
		Name: "Nano Interactive",
	}

	if err = pub.Publish(context.Background(), message); err != nil {
		panic(err)
	}

	if err = pub.Close(); err != nil {
		panic(err)
	}
}
