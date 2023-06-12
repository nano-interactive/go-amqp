package publisher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
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

	Pub[T Message] interface {
		io.Closer
		Publish(ctx context.Context, msg T) error
	}

	Publisher[T Message] struct {
		conn       connection.Connection
		serializer serializer.Serializer[T]
		ch         *amqp091.Channel
		cancel     context.CancelFunc
		ready      sync.RWMutex
		wg         sync.WaitGroup
	}
)

func (publisher *Publisher[T]) onConnectionReady(cfg Config[T]) connection.OnConnectionReady {
	var msg T
	exchangeName := msg.GetExchangeName()
	exchangeType := msg.GetExchangeType().String()

	return func(ctx context.Context, connection *amqp091.Connection) error {
		chOrigin, notifyClose, err := newChannel(
			connection,
			exchangeName,
			exchangeType,
			cfg.logger,
		)
		if err != nil {
			return err
		}

		publisher.wg.Add(1)
		publisher.ch = chOrigin
		publisher.ready.Unlock()
		go func() {
			defer publisher.wg.Done()
			errCh := make(chan error)
			defer close(errCh)

			for {
				select {
				case <-ctx.Done():
					publisher.ready.Lock()
					if !publisher.ch.IsClosed() {
						if err = publisher.ch.Close(); err != nil {
							cfg.logger.Error("Failed to close channel: %v", err)
						}
					}
					publisher.ready.Unlock()
					return
				case <-errCh:
					publisher.ready.Lock()

					chOrigin, notifyClose, err = newChannel(
						connection,
						exchangeName,
						msg.GetExchangeType().String(),
						cfg.logger,
					)

					if err != nil {
						errCh <- err
						publisher.ready.Unlock()
						continue
					}

					publisher.ch = chOrigin
					publisher.ready.Unlock()

				case err, ok := <-notifyClose:
					if !ok {
						return
					}

					if connection.IsClosed() {
						cfg.logger.Error("Connection closed")
						return
					}

					cfg.logger.Error("Channel Error: %v", err)
					// When connection is still open and channel is closed we need to create new channel
					// and throw away the old one
					if errors.Is(err, amqp091.ErrClosed) {
						errCh <- err
					}
				}
			}
		}()
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

	ctx, cancel := context.WithCancel(cfg.ctx)

	publisher := &Publisher[T]{
		serializer: cfg.serializer,
		cancel:     cancel,
	}

	conn, err := connection.New(ctx, cfg.connectionOptions, connection.Events{
		OnConnectionReady: publisher.onConnectionReady(cfg),
		OnBeforeConnectionReady: func(ctx context.Context) error {
			publisher.ready.Lock()
			return nil
		},
		OnError: cfg.onError,
	})
	if err != nil {
		return nil, err
	}

	publisher.conn = conn
	return publisher, nil
}

func (p *Publisher[T]) Publish(ctx context.Context, msg T) error {
	p.ready.RLock()
	defer p.ready.RUnlock()
	body, err := p.serializer.Marshal(msg)
	if err != nil {
		return err
	}

	return p.ch.PublishWithContext(
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
	p.cancel()
	p.wg.Wait()
	return p.conn.Close()
}
