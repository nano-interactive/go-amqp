package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
)

type (
	Message interface{}

	Sub[T Message] interface {
		io.Closer
	}

	Consumer[T Message] struct {
		queues *queue
	}
)

func NewRaw[T Message](handler RawHandler, options ...Option) (Consumer[T], error) {
	var msg T

	if reflect.ValueOf(msg).Kind() == reflect.Ptr {
		return Consumer[T]{}, errors.New("message type must be a value type")
	}

	cfg := Config{
		queueConfig: QueueConfig{
			PrefetchCount: 128,
			Workers:       1,
			QueueName:     "",
		},
		logger: amqp.EmptyLogger{},
		ctx:    context.Background(),
		onError: func(err error) {
			if errors.Is(err, connection.ErrRetriesExhausted) {
				panic(err)
			}

			fmt.Fprintf(os.Stderr, "[ERROR]: An error has occurred! %v\n", err)
		},
		connectionOptions: connection.DefaultConfig,
		onMessageError:    nil,
		onListenerStart:   nil,
		onListenerExit:    nil,
	}

	for _, o := range options {
		o(&cfg)
	}

	if cfg.queueConfig.QueueName == "" {
		return Consumer[T]{}, errors.New("queue name is required... Please call WithQueueName(queueName) option function")
	}

	queue, err := newQueue(cfg.ctx, cfg, handler)
	if err != nil {
		return Consumer[T]{}, err
	}

	return Consumer[T]{
		queues: queue,
	}, nil
}

func NewRawFunc[T Message](h RawHandlerFunc, options ...Option) (Consumer[T], error) {
	return NewRaw[T](h, options...)
}

func NewFunc[T Message](h HandlerFunc[T], options ...Option) (Consumer[T], error) {
	var privHandler RawHandler

	cfg := Config{}

	for _, o := range options {
		o(&cfg)
	}

	if cfg.retryCount > 0 {
		privHandler = retryHandler[T]{
			handler:    h,
			retryCount: uint32(cfg.retryCount),
		}
	} else {
		privHandler = handler[T]{
			handler: h,
		}
	}

	return NewRaw[T](privHandler, options...)
}

func New[T Message](h Handler[T], options ...Option) (Consumer[T], error) {
	var privHandler RawHandler

	cfg := Config{}

	for _, o := range options {
		o(&cfg)
	}

	if cfg.retryCount > 0 {
		privHandler = retryHandler[T]{
			handler:    h,
			retryCount: uint32(cfg.retryCount),
		}
	} else {
		privHandler = handler[T]{
			handler: h,
		}
	}

	return NewRaw[T](privHandler, options...)
}

func (c Consumer[T]) Close() error {
	if err := c.queues.Close(); err != nil {
		return err
	}

	return nil
}
