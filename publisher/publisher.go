package publisher

import (
	"context"
	"errors"
	"github.com/nano-interactive/go-amqp"
	"sync"
	"time"

	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/serializer"
	"github.com/rabbitmq/amqp091-go"
)

type (
	Message interface {
		GetExchangeName() string
		GetExchangeType() ExchangeType
	}

	Publisher[T Message] struct {
		errCh      chan error
		publish    chan T
		m          sync.RWMutex
		pool       *connection.Pool
		serializer serializer.Serializer[T]
		channels   []*amqp091.Channel
		logger     amqp.Logger
	}
)

func New[T Message](ctx context.Context, pool *connection.Pool, options ...Option[T]) (*Publisher[T], error) {
	cfg := Config[T]{
		serializer:        serializer.JsonSerializer[T]{},
		logger:            &amqp.EmptyLogger{},
		connectionTimeout: 1 * time.Second,
		publishers:        1,
		publishCapacity:   128,
	}

	var msg T

	for _, option := range options {
		option(&cfg)
	}

	newCtx, cancel := context.WithTimeout(ctx, cfg.connectionTimeout)
	defer cancel()

	conn, err := pool.Get(newCtx)

	if err != nil {
		return nil, err
	}

	defer pool.Release(conn)

	ch, err := conn.RawConnection().Channel()
	if err != nil {
		return nil, err
	}

	if err := ch.Confirm(false); err != nil {
		return nil, err
	}

	publisher := &Publisher[T]{
		pool:       pool,
		serializer: cfg.serializer,
		logger:     cfg.logger,
		channels:   make([]*amqp091.Channel, 0, 100),
		m:          sync.RWMutex{},
		errCh:      make(chan error, 1),
		publish:    make(chan T, 1000),
	}

	newErrCh := ch.NotifyClose(make(chan *amqp091.Error, 1))
	go recreate(ctx, publisher, cfg, pool, newErrCh)

	//ret := make(chan amqp091.Return)

	//ch.NotifyReturn(ret)
	//
	//go func() {
	//	for val := range ret {
	//		fmt.Printf("Returned: %s\n", string(val.Body))
	//	}
	//}()

	err = ch.ExchangeDeclare(
		msg.GetExchangeName(),
		msg.GetExchangeType().String(),
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	return publisher, nil
}

func (p *Publisher[T]) Publish(ctx context.Context, msg T) error {
	select {
	case err, more := <-p.errCh:
		if !more {
			return errors.New("amqp -> channel closed")
		}
		return err
	default:
		// Continue -> no error
	}

	p.m.RLock()
	defer p.m.RUnlock()
	body, err := p.serializer.Marshal(msg)

	if err != nil {
		return err
	}

	publishing := amqp091.Publishing{
		ContentType:  p.serializer.GetContentType(),
		DeliveryMode: amqp091.Persistent,
		Timestamp:    time.Now(),
		Body:         body,
	}

	name := msg.GetExchangeName()

	confirm, err := p.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		name,
		"",
		true,
		false,
		publishing,
	)

	if err != nil {
		return err
	}

	if !confirm.Wait() {
		return errors.New("server not confirmed the message")
	}

	return nil
}

func (p *Publisher[T]) Close() error {
	close(p.errCh)

	if !p.channel.IsClosed() {
		return p.channel.Close()
	}

	return nil
}
