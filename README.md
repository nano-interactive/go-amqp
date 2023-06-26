# Go Library Template

[![Testing](https://github.com/nano-interactive/go-amqp/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/nano-interactive/go-amqp/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/nano-interactive/go-amqp/branch/master/graph/badge.svg?token=JQTAGQ11DS)](https://codecov.io/gh/nano-interactive/go-amqp)
[![Go Report Card](https://goreportcard.com/badge/github.com/nano-interactive/go-amqp)](https://goreportcard.com/report/github.com/nano-interactive/go-amqp)


# Introduction

Having a distributed system with a message broker and no good wrappers for AMQP can be a hard work to develop, maintain and keeps the system running.
Working with async protocol in a language that does not support async code is a nightmare, especially with system that need to run 24/7/365. A lot of things can go wrong (e.g connection breaks, channel closes, memory leaks ...).

## Goals of the library

Goals with our AMQP wrapper are, to provide the most efficient way possible to consume and publish messages, with very simple API thats really hard to screw up. By providing 2 simple interfaces for `publishing` and `consuming` messages from any `AMQP` message broker

```go
// Consumer/Subscriber
type Sub[T Message] interface { // Message = any
    io.Closer
}

type Pub[T any] interface {
    io.Closer
    Publish(ctx context.Context, msg T) error
}
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
        Vhost:             "/",
        ConnectionName:    "go-amqp-consumer",
        ReconnectRetry:    10,
        ReconnectInterval: 1 * time.Second,
        Channels:          1000,
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
	fmt.Printf("[INFO] Message received: %d %s\n", cnt.Load(), msg.Name)
	return nil
}
```

We are following go's http module handler interface, with Handler and HandlerFunc, this means handlers can be created with a struct and method on it:

```go

type Message {
    Name string `json:"name"`
}

type Handler struct{}

func (h Handler) Handle(ctx context.Context, msg Message) error {
	fmt.Printf("[INFO] Message received: %d %s\n", cnt.Load(), msg.Name)
	return nil
}

```