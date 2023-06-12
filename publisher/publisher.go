package publisher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp"
	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/serializer"
)

var ErrChannelNotReady = errors.New("publishing channel is not ready")

type (
	Message interface {
		GetExchangeName() string
		GetExchangeType() ExchangeType
		GetRoutingKey() string
	}

	MessageRoutingKey interface {
		RoutingKey() string
	}

	Pub[T Message] interface {
		io.Closer
		Publish(ctx context.Context, msg T) error
	}

	Publisher[T Message] struct {
		conn       connection.Connection
		ch         atomic.Pointer[amqp091.Channel]
		serializer serializer.Serializer[T]
		ready      atomic.Bool
	}
)

func (publisher *Publisher[T]) onConnectionReady(cfg Config[T]) connection.OnConnectionReady {
	var msg T
	exchangeName := msg.GetExchangeName()
	exchangeType := msg.GetExchangeType().String()

	return func(ctx context.Context, connection *amqp091.Connection) error {
		publisher.ready.Store(false)
		chOrigin, notifyClose, err := newChannel(
			connection,
			exchangeName,
			exchangeType,
			cfg.logger,
		)
		if err != nil {
			return err
		}

		go func() {
			errCh := make(chan error)
			defer close(errCh)

			for notifyClose != nil {
				select {
				case <-ctx.Done():
					return
				case <-errCh:
					publisher.ready.Store(false)

					chOrigin, notifyClose, err = newChannel(
						connection,
						exchangeName,
						msg.GetExchangeType().String(),
						cfg.logger,
					)

					if err != nil {
						errCh <- err
						continue
					}

					publisher.ch.Store(chOrigin)
					publisher.ready.Store(true)

				case err, ok := <-notifyClose:
					if !ok {
						notifyClose = nil
						return
					}

					// When connection is still open and channel is closed we need to create new channel
					// and throw away the old one
					if errors.Is(err, amqp091.ErrClosed) && !connection.IsClosed() && chOrigin.IsClosed() {
						errCh <- err
					}
				}
			}
		}()

		publisher.ch.Store(chOrigin)
		publisher.ready.Store(true)

		return nil
	}
}

func newChannel(
	connection *amqp091.Connection,
	exchangeName, exchangeType string,
	logger amqp.Logger,
) (*amqp091.Channel, chan *amqp091.Error, error) {
	ch, err := connection.Channel()
	if err != nil {
		logger.Error("Failed to get channel: %v", err)
		return nil, nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		logger.Error("Failed to declare exchange: %s(%s) %v", exchangeName, exchangeType, err)
		return nil, nil, err
	}

	notifyClose := ch.NotifyClose(make(chan *amqp091.Error))

	return ch, notifyClose, nil
}

func New[T Message](options ...Option[T]) (*Publisher[T], error) {
	cfg := Config[T]{
		serializer:        serializer.JsonSerializer[T]{},
		logger:            amqp.EmptyLogger{},
		messageBuffering:  1,
		connectionOptions: connection.DefaultConfig,
		ctx:               context.Background(),
		onError: func(err error) {
			if errors.Is(err, connection.ErrRetriesExhausted) {
				panic(err)
			}

			fmt.Fprintf(os.Stderr, "[ERROR]: An error has occurred! %v\n", err)
		},
	}

	for _, option := range options {
		option(&cfg)
	}

	publisher := &Publisher[T]{
		serializer: cfg.serializer,
		ch:         atomic.Pointer[amqp091.Channel]{},
		ready:      atomic.Bool{},
	}

	onReady := publisher.onConnectionReady(cfg)
	onReadyCh := make(chan struct{}, 1)

	conn, err := connection.New(cfg.ctx, cfg.connectionOptions, connection.Events{
		OnConnectionReady: func(ctx context.Context, c *amqp091.Connection) error {
			err := onReady(ctx, c)
			defer close(onReadyCh)

			if err != nil {
				return err
			}

			onReadyCh <- struct{}{}

			return nil
		},
		OnError: cfg.onError,
	})
	if err != nil {
		return nil, err
	}

	<-onReadyCh
	publisher.conn = conn
	return publisher, nil
}

func (p *Publisher[T]) Publish(ctx context.Context, msg T) error {
	if !p.ready.Load() {
		return ErrChannelNotReady
	}

	body, err := p.serializer.Marshal(msg)
	if err != nil {
		return err
	}

	return p.ch.Load().PublishWithContext(
		ctx,
		msg.GetExchangeName(),
		msg.GetRoutingKey(),
		true,
		false,
		amqp091.Publishing{
			ContentType:  p.serializer.GetContentType(),
			DeliveryMode: amqp091.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
}

func (p *Publisher[T]) Close() error {
	return p.conn.Close()
}
