# Go Library Template

[![Testing](https://github.com/nano-interactive/go-amqp/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/nano-interactive/go-amqp/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/nano-interactive/go-amqp/branch/master/graph/badge.svg?token=JQTAGQ11DS)](https://codecov.io/gh/nano-interactive/go-amqp)
[![Go Report Card](https://goreportcard.com/badge/github.com/nano-interactive/go-amqp)](https://goreportcard.com/report/github.com/nano-interactive/go-amqp)

# Introduction

Having a distributed system with a message broker and no good wrappers for AMQP can be a hard work to develop, maintain and keeps the system running.
Working with async protocol in a language that does not support async code is a nightmare, especially with system that need to run 24/7/365. A lot of things can go wrong (e.g connection breaks, channel closes, memory leaks ...).

## Goals of the library

Goals with our AMQP wrapper are, to provide the most efficient way possible to consume and publish messages, with very simple API thats really hard to screw up. By providing 1 simple interface for `publishing` messages from any `AMQP` message broker, and only function to start `consumers`. `Consumer[T]` has a size of one pointer (8 bytes), and its used just to pass `T Message` and provide type safety to the underlying handler and to `Close` the workers running on multiple threads.
This makes those two very easy to `Mock` for unit testing, as `Pub[T]` is an interface with only one method, and `Consumer[T]` doesn't need to be tested, as it by it's self does nothing, only thing to test for `consumer` is it's handler that users of this library create themselfs.

## Install

To install the library just use `go get`

```sh
go get github.com/nano-interactive/go-amqp/v2

```

```go

// Publisher
type Pub[T any] interface {
    Publish(ctx context.Context, msg T) error
}

// Creator functions for Consumer
func New[T Message](Handler[T], QueueDeclare, ...Option[T]) (Consumer[T], error)
func NewFunc[T Message](HandlerFunc[T], QueueDeclare, ...Option[T]) (Consumer[T], error)

// **SHOULD BE USED WITH CARE**
func NewRawFunc[T Message](RawHandlerFunc, QueueDeclare, ...Option[T]) (Consumer[T], error)
func NewRaw[T Message](RawHandler, QueueDeclare, ...Option[T]) (Consumer[T], error)

```

we made it very easy to develop complex systems. By introducing generics we provide type-safety in consumers and publishers, when publisher is declared it can be used to publish only one type of message (provided as generic parameter), same applies to consumer -> by providing generic parameter, basic handler is type safe and messages with only type `T` will be accepted into the `Handler`.

### Consumer Example

By making consumers non-blocking, we give the user any way they like to wait for messages on some handler (context, signals, sleep...),
this allows us to abstract multithreading away from the user, and to provide the nice interface for handling messages, all burdon of concurrency
is places in the library so that the users of the library don't even think about it.

#### Declaring Consumers

```go

// consumer (c) is non blocking
c, err := consumer.NewFunc(
    handler,
    consumer.QueueDeclare{QueueName: "testing_queue"},
    consumer.WithOnMessageError[Message](func(ctx context.Context, d *amqp091.Delivery, err error) {
        fmt.Fprintf(os.Stderr, "[ERROR] Message error: %s\n", err)
    }),
    consumer.WithConnectionOptions[Message](connection.Config{
        Host:              "127.0.0.1",
        Port:              5672,
        User:              "guest",
        Password:          "guest",
        ConnectionName:    "go-amqp-consumer",
    }),
)

time.Sleep(100*time.Second)

c.Close()

```

#### Declaring Handler

There are multiple ways of declaring message handlers. Simplest way is to define a function with the following signature:

```go

type Message {
    Name string `json:"name"`
}

func handler(ctx context.Context, msg Message) error {
	fmt.Printf("[INFO] Message received: %s\n", msg.Name)
	return nil
}
```

We are following go's http module handler interface, with Handler and HandlerFunc, this means handlers can be created with a struct and method on it:

```go

type Message {
    Name string `json:"name"`
}

type MyHandler struct{}

func (h MyHandler) Handle(ctx context.Context, msg Message) error {
	fmt.Printf("[INFO] Message received: %s\n", msg.Name)
	return nil
}

```

This is all based on RawHandler, as this library does not go limit the power of AMQP library
In `RawHandler` implementation you as the user of the library have to parse, process and acknowledge the AMQP message. **Be careful!**
**This is not covered with our API stability as `*amqp091.Delivery` can change**

> This is how our internal handler wrapper are implemented, for the most usecases there is no need to implement `RawHandler`.

```go

type MyRawHandler struct{}

func (h MyRawHandler) Handle(ctx context.Context, d *amqp091.Delivery) error {
    d.Ack(false)
    return nil
}
```

**For more options check `consumer.With*` methods**

### Publisher Example

Publising message is simple, the abstraction is very simple

- First this is to create a publisher with exchange name

```go
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

```

- When publisher is created, publishing a message is simple

```go

message := Message{
    Name: "Nano Interactive",
}

if err = pub.Publish(context.Background(), message); err != nil {
    panic(err)
}

```

- And **DO NOT FORGET TO CLOSE**
If you don't close the publisher some messages might be lost, this is how asynchronous protocols work.
Not all sent messages are acknowledged immediatly.

```go
if err := pub.Close(); err != nil {
    panic(err)
}

```


### Testing Example

`go-amqp` library provides a few testing helpers for integration testing. Functions for setting up AMQP channels, queues and exchanges (binding them together), publishing and consuming messages for asserting.