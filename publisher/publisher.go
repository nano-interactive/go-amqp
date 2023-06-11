package publisher

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
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

	Pub[T Message] interface {
		io.Closer
		Publish(ctx context.Context, msg T) error
	}

	Publisher[T Message] struct {
		conn       connection.Connection
		ch         *atomic.Pointer[amqp091.Channel]
		serializer serializer.Serializer[T]
		ready      *atomic.Bool
	}
)

func New[T Message](ctx context.Context, conn connection.Connection, options ...Option[T]) (*Publisher[T], error) {
	cfg := Config[T]{
		serializer:        serializer.JsonSerializer[T]{},
		logger:            amqp.EmptyLogger{},
		connectionTimeout: 1 * time.Second,
		messageBuffering:  1,
	}

	for _, option := range options {
		option(&cfg)
	}

	var msg T

	exchangeName := msg.GetExchangeName()

	chOrigin, err := conn.RawConnection().Channel()
	if err != nil {
		cfg.logger.Error("Failed to get channel: %v", err)
		return nil, err
	}

	ch := &atomic.Pointer[amqp091.Channel]{}
	ch.Store(chOrigin)

	notifyClose := make(chan *amqp091.Error)
	chOrigin.NotifyClose(notifyClose)

	ready := &atomic.Bool{}
	ready.Store(true)
	go func() {
		errCh := make(chan error, 1)
		defer close(errCh)
		for {
			select {
			case <-ctx.Done():
				return
			case err = <-errCh:
				ready.Store(false)
				chOrigin, err = conn.RawConnection().Channel()

				if err != nil {
					cfg.logger.Error("Failed to get channel: %v", err)
					errCh <- err
					continue
				}
				notifyClose = chOrigin.NotifyClose(make(chan *amqp091.Error))
				ch.Store(chOrigin)
				ready.Store(true)
			case err, ok := <-notifyClose:
				if !ok {
					return
				}

				errCh <- err
			}
		}
	}()

	err = chOrigin.ExchangeDeclare(
		exchangeName,
		msg.GetExchangeType().String(),
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		cfg.logger.Error("Failed to declare exchange: %s %v", exchangeName, err)
		return nil, err
	}

	return &Publisher[T]{
		conn:       conn,
		ch:         ch,
		serializer: cfg.serializer,
		ready:      ready,
	}, nil
}

func (p *Publisher[T]) Publish(ctx context.Context, msg T) error {
	if !p.ready.Load() {
		return errors.New("publishing channel is not ready")
	}

	body, err := p.serializer.Marshal(msg)
	if err != nil {
		return err
	}

	exchange := msg.GetExchangeName()

	for {
		ch := p.ch.Load()

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err = ch.PublishWithContext(
			ctx,
			exchange,
			"",
			true,
			false,
			amqp091.Publishing{
				ContentType:  p.serializer.GetContentType(),
				DeliveryMode: amqp091.Persistent,
				Timestamp:    time.Now(),
				Body:         body,
			},
		)

		if err == nil {
			return nil
		}
	}
}

func (p *Publisher[T]) Close() error {
	return p.conn.Close()
}
