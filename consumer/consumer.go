package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/serializer"
)

var (
	_ io.Closer = Consumer[any]{}
	_ io.Closer = (*Consumer[any])(nil)
)

type (
	Message any

	Consumer[T Message] struct {
		watcher      *semaphore.Weighted
		cfg          *Config[T]
		queueDeclare *QueueDeclare
		handler      RawHandler
	}
)

func NewRaw[T Message](
	handler RawHandler,
	connectionOptions connection.Config,
	queueDeclare QueueDeclare,
	options ...Option[T],
) (Consumer[T], error) {
	var msg T

	if reflect.ValueOf(msg).Kind() == reflect.Ptr {
		return Consumer[T]{}, ErrMessageTypeInvalid
	}

	cfg := &Config[T]{
		queueConfig: QueueConfig{
			PrefetchCount: 128,
			Workers:       1,
		},
		retryCount: 1,
		serializer: serializer.JSON[T]{},
		onError: func(err error) {
			if errors.Is(err, connection.ErrRetriesExhausted) {
				panic(err)
			}
		},
		connectionOptions: connectionOptions,
		onMessageError:    nil,
		onListenerStart:   nil,
		onListenerExit:    nil,
	}

	for _, o := range options {
		o(cfg)
	}

	if queueDeclare.QueueName == "" {
		return Consumer[T]{}, ErrQueueNameRequired
	}

	if cfg.onMessageError == nil {
		return Consumer[T]{}, ErrOnMessageCallbackRequired
	}

	return Consumer[T]{
		watcher:      semaphore.NewWeighted(int64(cfg.queueConfig.Workers)),
		cfg:          cfg,
		queueDeclare: &queueDeclare,
		handler:      handler,
	}, nil
}

func NewRawFunc[T Message](h RawHandlerFunc, connectionOptions connection.Config, queueDeclare QueueDeclare, options ...Option[T]) (Consumer[T], error) {
	return NewRaw(h, connectionOptions, queueDeclare, options...)
}

func NewFunc[T Message](
	h HandlerFunc[T],
	connectionOptions connection.Config,
	queueDeclare QueueDeclare,
	options ...Option[T],
) (Consumer[T], error) {
	cfg := Config[T]{}

	for _, o := range options {
		o(&cfg)
	}

	var (
		rawHandler RawHandler
		s          serializer.Serializer[T]
	)

	if cfg.serializer == nil {
		s = serializer.JSON[T]{}
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

	return NewRaw(rawHandler, connectionOptions, queueDeclare, options...)
}

func New[T Message](
	h Handler[T],
	connectionOptions connection.Config,
	queueDeclare QueueDeclare,
	options ...Option[T],
) (Consumer[T], error) {
	cfg := Config[T]{}

	for _, o := range options {
		o(&cfg)
	}

	var (
		rawHandler RawHandler
		s          serializer.Serializer[T]
	)

	if cfg.serializer == nil {
		s = serializer.JSON[T]{}
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

	return NewRaw(rawHandler, connectionOptions, queueDeclare, options...)
}

func (c Consumer[T]) CloseWithContext(ctx context.Context) error {
	// Try to acquire all permits with a timeout
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := c.watcher.Acquire(ctx, int64(c.cfg.queueConfig.Workers)); err != nil {
		return fmt.Errorf("failed to acquire all watchers: %w", err)
	}

	// Release all permits to ensure clean state
	c.watcher.Release(int64(c.cfg.queueConfig.Workers))
	return nil
}

func (c Consumer[T]) Close() error {
	return c.CloseWithContext(context.Background())
}
