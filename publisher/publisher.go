package publisher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"github.com/nano-interactive/go-amqp/v3/connection"
	"github.com/nano-interactive/go-amqp/v3/serializer"
)

var (
	ErrChannelNotReady = errors.New("publishing channel is not ready")
	ErrClosed          = errors.New("publisher is closed")
)

var (
	_ Pub[any]  = (*Publisher[any])(nil)
	_ io.Closer = (*Publisher[any])(nil)
)

type (
	Pub[T any] interface {
		Publish(context.Context, T, ...PublishConfig) error
	}

	Publisher[T any] struct {
		serializer   serializer.Serializer[T]
		conn         *connection.Connection
		ch           atomic.Pointer[amqp091.Channel]
		cancel       context.CancelFunc
		exchangeName string
		routingKey   string
		wg           sync.WaitGroup
		closing      atomic.Bool
		gettingCh    atomic.Bool
	}

	ExchangeDeclare struct {
		Args       amqp091.Table
		name       string
		RoutingKey string
		Type       ExchangeType
		Durable    bool
		AutoDelete bool
		Internal   bool
		NoWait     bool
	}
)

func (e ExchangeDeclare) declare(ch *amqp091.Channel) error {
	err := ch.ExchangeDeclare(e.name, e.Type.String(), e.Durable, e.AutoDelete, e.Internal, e.NoWait, e.Args)
	if err != nil {
		return err
	}

	return nil
}

func (p *Publisher[T]) swapChannel(connection *amqp091.Connection, cfg Config[T]) (chan *amqp091.Error, error) {
	p.gettingCh.Store(true)
	chOrigin, notifyClose, err := newChannel(connection, cfg.exchange)
	if err != nil {
		return nil, err
	}

	p.ch.Store(chOrigin)
	p.gettingCh.Store(false)
	return notifyClose, nil
}

func (p *Publisher[T]) connectionReadyWorker(ctx context.Context, conn *amqp091.Connection, notifyClose chan *amqp091.Error, cfg Config[T]) {
	defer p.wg.Done()
	errCh := make(chan error)
	defer close(errCh)
	var err error

	defer func() {
		p.closing.Store(true)
		ch := p.ch.Load()
		if !ch.IsClosed() {
			_ = ch.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-errCh:
			notifyClose, err = p.swapChannel(conn, cfg)
			if err != nil {
				errCh <- err
			}
		case err, ok := <-notifyClose:
			if !ok {
				return
			}

			if conn.IsClosed() {
				return
			}

			p.gettingCh.CompareAndSwap(false, true)

			// When connection is still open and channel is closed we need to create new channel
			// and throw away the old one
			if errors.Is(err, amqp091.ErrClosed) {
				errCh <- err
			}
		}
	}
}

func (p *Publisher[T]) onConnectionReady(cfg Config[T]) connection.OnConnectionReady {
	return func(ctx context.Context, connection *amqp091.Connection) error {
		p.wg.Add(1)
		notifyClose, err := p.swapChannel(connection, cfg)
		if err != nil {
			return err
		}

		go p.connectionReadyWorker(ctx, connection, notifyClose, cfg)
		return nil
	}
}

func newChannel(
	connection *amqp091.Connection,
	exchange ExchangeDeclare,
) (*amqp091.Channel, chan *amqp091.Error, error) {
	ch, err := connection.Channel()
	if err != nil {
		return nil, nil, err
	}

	if err := exchange.declare(ch); err != nil {
		return nil, nil, err
	}

	notifyClose := ch.NotifyClose(make(chan *amqp091.Error))

	return ch, notifyClose, nil
}

func New[T any](exchangeName string, options ...Option[T]) (*Publisher[T], error) {
	if exchangeName == "" {
		return nil, ErrExchangeNameRequired
	}

	cfg := Config[T]{
		serializer:        serializer.JSON[T]{},
		messageBuffering:  1,
		connectionOptions: connection.DefaultConfig,
		ctx:               context.Background(),
		exchange: ExchangeDeclare{
			name:       exchangeName,
			RoutingKey: "",
			Type:       ExchangeTypeFanout,
			Durable:    true,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
			Args:       nil,
		},
		onError: func(err error) {
			if errors.Is(err, connection.ErrRetriesExhausted) {
				panic(err)
			}

			_, _ = fmt.Fprintf(os.Stderr, "[ERROR]: An error has occurred! %v\n", err)
		},
	}

	for _, option := range options {
		option(&cfg)
	}

	ctx, cancel := context.WithCancel(cfg.ctx)

	publisher := &Publisher[T]{
		serializer:   cfg.serializer,
		cancel:       cancel,
		exchangeName: exchangeName,
		routingKey:   cfg.exchange.RoutingKey,
	}

	conn, err := connection.New(ctx, cfg.connectionOptions, connection.Events{
		OnConnectionReady: publisher.onConnectionReady(cfg),
		OnBeforeConnectionReady: func(_ context.Context) error {
			publisher.gettingCh.Store(true)
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

type PublishConfig struct{}

func (p *Publisher[T]) Publish(ctx context.Context, msg T, _ ...PublishConfig) error {
	if p.conn.IsClosed() {
		return ErrClosed
	}

	if p.gettingCh.Load() {
		return ErrChannelNotReady
	}

	body, err := p.serializer.Marshal(msg)
	if err != nil {
		return err
	}

	return (*p.ch.Load()).PublishWithContext(
		ctx,
		p.exchangeName,
		p.routingKey,
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
	p.closing.Store(true)
	p.cancel()
	p.wg.Wait()
	return p.conn.Close()
}
