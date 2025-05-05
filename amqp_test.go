package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/consumer"
	"github.com/nano-interactive/go-amqp/v3/publisher"
	amqptesting "github.com/nano-interactive/go-amqp/v3/testing"
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

func TestConsumer(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mappings := amqptesting.NewMappings(t).
		AddMapping("test_exchange", "test_queue")

	c, err := consumer.NewFunc(
		func(_ context.Context, msg Message) error {
			t.Logf("Message received: %s", msg.Name)
			return nil
		},
		connection.DefaultConfig,
		consumer.QueueDeclare{QueueName: mappings.Queue("test_queue")},
		consumer.WithOnMessageError[Message](func(_ context.Context, _ *amqp091.Delivery, err error) {
			t.Logf("Message error: %s", err)
		}),
	)
	assert.NoError(err)
	assert.NotNil(c)

	go func() {
		if err := c.Start(ctx); err != nil {
			t.Logf("Consumer error: %s", err)
		}
	}()

	// Wait for consumer to start
	time.Sleep(100 * time.Millisecond)

	// Cleanup
	assert.NoError(c.Close())
}

type TestHandler struct{}

func (h TestHandler) Handle(_ context.Context, msg Message) error {
	return nil
}

func TestConsumerWithHandler(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mappings := amqptesting.NewMappings(t).
		AddMapping("test_exchange", "test_queue")

	handler := TestHandler{}

	c, err := consumer.New[Message](
		handler,
		connection.DefaultConfig,
		consumer.QueueDeclare{QueueName: mappings.Queue("test_queue")},
		consumer.WithOnMessageError[Message](func(_ context.Context, _ *amqp091.Delivery, err error) {
			t.Logf("Message error: %s", err)
		}),
	)
	assert.NoError(err)
	assert.NotNil(c)

	go func() {
		if err := c.Start(ctx); err != nil {
			t.Logf("Consumer error: %s", err)
		}
	}()

	// Wait for consumer to start
	time.Sleep(100 * time.Millisecond)

	// Cleanup
	assert.NoError(c.Close())
}

type TestRawHandler struct{}

func (h TestRawHandler) Handle(_ context.Context, d *amqp091.Delivery) error {
	return d.Ack(false)
}

func TestConsumerWithRawHandler(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mappings := amqptesting.NewMappings(t).
		AddMapping("test_exchange", "test_queue")

	handler := TestRawHandler{}

	c, err := consumer.NewRaw(
		handler,
		connection.DefaultConfig,
		consumer.QueueDeclare{QueueName: mappings.Queue("test_queue")},
		consumer.WithOnMessageError[Message](func(_ context.Context, _ *amqp091.Delivery, err error) {
			t.Logf("Message error: %s", err)
		}),
	)
	assert.NoError(err)
	assert.NotNil(c)

	go func() {
		if err := c.Start(ctx); err != nil {
			t.Logf("Consumer error: %s", err)
		}
	}()

	// Wait for consumer to start
	time.Sleep(100 * time.Millisecond)

	// Cleanup
	assert.NoError(c.Close())
}

func TestPublisher(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mappings := amqptesting.NewMappings(t).
		AddMapping("test_exchange", "test_queue")

	pub, err := publisher.New[Message](
		ctx,
		connection.DefaultConfig,
		mappings.Exchange("test_exchange"),
	)
	assert.NoError(err)
	assert.NotNil(pub)

	message := Message{
		Name: "Test Message",
	}

	assert.NoError(pub.Publish(ctx, message))

	// Cleanup
	assert.NoError(pub.Close())
}

func TestConsumerWithSignal(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Simulate signal channel
	sig := make(chan os.Signal, 1)

	c, err := consumer.NewFunc(handler,
		connection.DefaultConfig,
		consumer.QueueDeclare{QueueName: "testing_queue"},
		consumer.WithOnMessageError[Message](func(_ context.Context, _ *amqp091.Delivery, err error) {
			t.Logf("Message error: %s", err)
		}),
	)
	assert.NoError(err)
	assert.NotNil(c)

	// Start consumer in goroutine
	go func() {
		if err := c.Start(ctx); err != nil {
			t.Logf("Consumer error: %s", err)
		}
	}()

	// Wait a bit for consumer to start
	time.Sleep(100 * time.Millisecond)

	// Simulate signal
	sig <- syscall.SIGTERM
	cancel()

	// Cleanup with timeout
	assert.NoError(c.Close())
}

func ExampleConsumer() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := consumer.NewFunc(handler,
		connection.DefaultConfig,
		consumer.QueueDeclare{QueueName: "testing_queue"},
		consumer.WithOnMessageError[Message](func(_ context.Context, _ *amqp091.Delivery, err error) {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Message error: %s\n", err)
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
	time.Sleep(time.Second)

	// Cleanup with timeout
	if err := c.Close(); err != nil {
		panic(err)
	}

	fmt.Println("[INFO] Consumer shut down cleanly")
	// Output:
	// [INFO] Consumer started
	// [INFO] Consumer shut down cleanly
}

type MyHandler struct{}

func (h MyHandler) Handle(_ context.Context, msg Message) error {
	defer cnt.Add(1)
	//nolint:forbidigo
	_, _ = fmt.Printf("[INFO] Message received: %d %s\n", cnt.Load(), msg.Name)
	return nil
}

func ExampleConsumerWithHandler() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := consumer.New[Message](MyHandler{},
		connection.DefaultConfig,
		consumer.QueueDeclare{QueueName: "testing_queue"},
		consumer.WithOnMessageError[Message](func(_ context.Context, _ *amqp091.Delivery, err error) {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Message error: %s\n", err)
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
	time.Sleep(time.Second)

	// Cleanup with timeout
	if err := c.Close(); err != nil {
		panic(err)
	}

	// Output:
	// [INFO] Consumer started
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := consumer.NewRaw(MyRawHandler{},
		connection.DefaultConfig,
		consumer.QueueDeclare{QueueName: "testing_queue"},
		consumer.WithOnMessageError[Message](func(_ context.Context, _ *amqp091.Delivery, err error) {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Message error: %s\n", err)
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
	time.Sleep(time.Second)

	// Cleanup with timeout
	if err := c.Close(); err != nil {
		panic(err)
	}

	// Output:
	// [INFO] Consumer started
}

func ExamplePublisher() {
	pub, err := publisher.New[Message](
		context.Background(),
		connection.DefaultConfig,
		"testing_publisher",
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

	// Output:
}
