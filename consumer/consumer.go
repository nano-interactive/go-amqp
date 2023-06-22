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
	"github.com/nano-interactive/go-amqp/serializer"
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

func NewRaw[T Message](handler RawHandler, queueDeclare QueueDeclare, options ...Option[T]) (Consumer[T], error) {
	var msg T

	if reflect.ValueOf(msg).Kind() == reflect.Ptr {
		return Consumer[T]{}, errors.New("message type must be a value type")
	}

	cfg := Config[T]{
		queueConfig: QueueConfig{
			PrefetchCount: 128,
			Workers:       1,
		},
		retryCount: 1,
		serializer: serializer.JsonSerializer[T]{},
		logger:     amqp.EmptyLogger{},
		ctx:        context.Background(),
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

	if queueDeclare.QueueName == "" {
		return Consumer[T]{}, errors.New("queue name is required... Please call WithQueueName(queueName) option function")
	}

	if cfg.onMessageError == nil {
		panic("onMessageError is required")
	}

	queue, err := newQueue(cfg.ctx, cfg, queueDeclare, handler)
	if err != nil {
		return Consumer[T]{}, err
	}

	return Consumer[T]{
		queues: queue,
	}, nil
}

func NewRawFunc[T Message](h RawHandlerFunc, queueDeclare QueueDeclare, options ...Option[T]) (Consumer[T], error) {
	return NewRaw[T](h, queueDeclare, options...)
}

func NewFunc[T Message](h HandlerFunc[T], queueDeclare QueueDeclare, options ...Option[T]) (Consumer[T], error) {
	cfg := Config[T]{}

	for _, o := range options {
		o(&cfg)
	}

	var (
		rawHandler RawHandler
		s          serializer.Serializer[T]
	)

	if cfg.serializer == nil {
		s = serializer.JsonSerializer[T]{}
	} else {
		s = cfg.serializer
	}

	privHandler := handler[T]{
		handler:    h,
		serializer: s,
	}

	if cfg.retryCount > 1 {
		rawHandler = retryHandler[T]{
			handler:    privHandler,
			retryCount: uint32(cfg.retryCount),
		}
	} else {
		rawHandler = privHandler
	}

	return NewRaw[T](rawHandler, queueDeclare, options...)
}

func New[T Message](h Handler[T], queueDeclare QueueDeclare, options ...Option[T]) (Consumer[T], error) {
	cfg := Config[T]{}

	for _, o := range options {
		o(&cfg)
	}

	var (
		rawHandler RawHandler
		s          serializer.Serializer[T]
	)

	if cfg.serializer == nil {
		s = serializer.JsonSerializer[T]{}
	} else {
		s = cfg.serializer
	}

	privHandler := handler[T]{
		handler:    h,
		serializer: s,
	}

	if cfg.retryCount > 1 {
		rawHandler = retryHandler[T]{
			handler:    privHandler,
			retryCount: uint32(cfg.retryCount),
		}
	} else {
		rawHandler = privHandler
	}

	return NewRaw[T](rawHandler, queueDeclare, options...)
}

func (c Consumer[T]) Close() error {
	if err := c.queues.Close(); err != nil {
		return err
	}

	return nil
}
