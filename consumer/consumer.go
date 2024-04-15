package consumer

import (
	"context"
	"errors"
	"golang.org/x/sync/semaphore"
	"io"
	"reflect"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/serializer"
)

var (
	_ io.Closer = Consumer[any]{}
	_ io.Closer = (*Consumer[any])(nil)
)

type (
	Message interface{}

	Consumer[T Message] struct {
		watcher      *semaphore.Weighted
		cfg          *Config[T]
		queueDeclare *QueueDeclare
		handler      RawHandler
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
		ctx:        context.Background(),
		onError: func(err error) {
			if errors.Is(err, connection.ErrRetriesExhausted) {
				panic(err)
			}
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
		return Consumer[T]{}, errors.New("q name is required... Please call WithQueueName(queueName) option function")
	}

	if cfg.onMessageError == nil {
		panic("onMessageError is required")
	}

	return Consumer[T]{
		watcher:      semaphore.NewWeighted(int64(cfg.queueConfig.Workers)),
		cfg:          &cfg,
		queueDeclare: &queueDeclare,
		handler:      handler,
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
			retryCount: cfg.retryCount,
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
			retryCount: cfg.retryCount,
		}
	} else {
		rawHandler = privHandler
	}

	return NewRaw[T](rawHandler, queueDeclare, options...)
}

func (c Consumer[T]) Close() error {
	return c.watcher.Acquire(context.Background(), int64(c.cfg.queueConfig.Workers))
}
