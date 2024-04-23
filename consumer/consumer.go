package consumer

import (
	"context"
	"errors"
	"reflect"

	"golang.org/x/sync/semaphore"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/serializer"
)

type (
	Message any

	Queue interface {
		QueueConfig() QueueDeclare
	}

	Consumer[T Message, Q Queue] struct {
		watcher      *semaphore.Weighted
		cfg          *Config[T]
		queueDeclare Q
		handler      RawHandler
	}
)

func NewRaw[T Message, Q Queue](
	handler RawHandler,
	connectionOptions connection.Config,
	queue Q,
	options ...Option[T],
) (Consumer[T, Q], error) {
	var msg T

	if reflect.ValueOf(msg).Kind() == reflect.Ptr {
		return Consumer[T, Q]{}, ErrMessageTypeInvalid
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

	queueDeclare := queue.QueueConfig()

	if queueDeclare.QueueName == "" {
		return Consumer[T, Q]{}, ErrQueueNameRequired
	}

	if cfg.onMessageError == nil {
		return Consumer[T, Q]{}, ErrOnMessageCallbackRequired
	}

	return Consumer[T, Q]{
		watcher:      semaphore.NewWeighted(int64(cfg.queueConfig.Workers)),
		cfg:          cfg,
		queueDeclare: queue,
		handler:      handler,
	}, nil
}

func NewRawFunc[T Message, Q Queue](h RawHandlerFunc, connectionOptions connection.Config, queueDeclare Q, options ...Option[T]) (Consumer[T, Q], error) {
	return NewRaw[T, Q](h, connectionOptions, queueDeclare, options...)
}

func NewFunc[T Message, Q Queue](
	h HandlerFunc[T],
	connectionOptions connection.Config,
	queueDeclare Q,
	options ...Option[T],
) (Consumer[T, Q], error) {
	return New[T, Q](h, connectionOptions, queueDeclare, options...)
}

func New[T Message, Q Queue](
	h Handler[T],
	connectionOptions connection.Config,
	queueDeclare Q,
	options ...Option[T],
) (Consumer[T, Q], error) {
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

func (c Consumer[T, Q]) CloseWithContext(ctx context.Context) error {
	return c.watcher.Acquire(ctx, int64(c.cfg.queueConfig.Workers))
}

func (c Consumer[T, Q]) Close() error {
	return c.CloseWithContext(context.Background())
}
