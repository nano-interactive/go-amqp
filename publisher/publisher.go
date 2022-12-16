package publisher

import (
	"context"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/serializer"
)

type (
	Message interface {
		GetExchangeName() string
		GetExchangeType() ExchangeType
	}

	publishing struct {
		amqp091.Publishing
		errCb func(error)
	}

	Publisher[T Message] struct {
		cancel     context.CancelFunc
		wg         *sync.WaitGroup
		conn       connection.Connection
		serializer serializer.Serializer[T]
		publish    chan publishing
		watchDog   chan struct{}
	}
)

func New[T Message](ctx context.Context, conn connection.Connection, options ...Option[T]) (*Publisher[T], error) {
	cfg := Config[T]{
		serializer:        serializer.JsonSerializer[T]{},
		logger:            &amqp.EmptyLogger{},
		connectionTimeout: 1 * time.Second,
		messageBuffering:  1,
	}

	for _, option := range options {
		option(&cfg)
	}

	publish := make(chan publishing, cfg.messageBuffering)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	newCtx, cancel := context.WithCancel(ctx)
	setupErrCh := make(chan error, 1)
	workerExitCh := make(chan struct{})
	go worker[T](newCtx, conn, wg, setupErrCh, publish, workerExitCh)

	if err := <-setupErrCh; err != nil {
		_ = conn.Close()
		cancel()
		wg.Wait()
		return nil, err
	}

	go watchdog[T](newCtx, conn, wg, publish, workerExitCh)

	return &Publisher[T]{
		cancel:     cancel,
		wg:         wg,
		conn:       conn,
		serializer: cfg.serializer,
		publish:    publish,
		watchDog:   workerExitCh,
	}, nil
}

func (p *Publisher[T]) Publish(ctx context.Context, msg T, errorCallback ...func(error)) error {
	body, err := p.serializer.Marshal(msg)

	if err != nil {
		return err
	}

	var errCb func(error)

	if len(errorCallback) > 0 {
		errCb = errorCallback[0]
	}

	pub := publishing{
		Publishing: amqp091.Publishing{
			ContentType:  p.serializer.GetContentType(),
			DeliveryMode: amqp091.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
		errCb: errCb,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.publish <- pub:
		return nil
	}
}

func (p *Publisher[T]) Close() error {
	p.cancel()
	p.wg.Wait()
	close(p.watchDog)
	return p.conn.Close()
}
