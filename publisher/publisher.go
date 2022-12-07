package publisher

import (
	"context"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/multierr"

	"github.com/nano-interactive/go-amqp/connection"
	"github.com/nano-interactive/go-amqp/serializer"
)

type (
	Message interface {
		GetExchangeName() string
	}

	Publisher[T Message] struct {
		exchangeName string
		connection   connection.Connection
		serializer   serializer.Serializer[T]
		exchange     *exchange
	}
)

func NewPublisher[T Message](ctx context.Context, options ...PublisherOption[T]) (*Publisher[T], error) {
	cfg := PublisherConfig[T]{
		serializer:       serializer.JsonSerializer[T]{},
		exchangeType:     ExchangeTypeFanout,
		exchangeName:     "default",
		connectionConfig: connection.DefaultConfig,
	}

	for _, option := range options {
		option(&cfg)
	}

	conn, err := connection.New(&cfg.connectionConfig)
	if err != nil {
		return nil, err
	}

	ch, err := conn.RawConnection().Channel()
	if err != nil {
		return nil, err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(cfg.exchangeName, cfg.exchangeType.String(), true, false, false, false, nil)

	if err != nil {
		return nil, err
	}

	ex, err := newExchange(conn, ctx, uint32(100))
	if err != nil {
		return nil, err
	}

	return &Publisher[T]{
		connection:   conn,
		serializer:   cfg.serializer,
		exchangeName: cfg.exchangeName,
		exchange:     ex,
	}, nil
}

func (p *Publisher[T]) Publish(ctx context.Context, msg T) error {
	ch, err := p.exchange.GetChannel()
	if err != nil {
		return err
	}

	ch.RLock()
	defer ch.RUnlock()

	body, err := p.serializer.Marshal(msg)
	if err != nil {
		return err
	}

	publishing := amqp091.Publishing{
		ContentType: p.serializer.GetContentType(),
		Timestamp:   time.Now(),
		Body:        body,
		// TODO ContentEncoding: , for compression
		DeliveryMode: amqp091.Persistent,
		Priority:     0, // TODO
	}

	if err := ch.channel.PublishWithContext(ctx, p.exchangeName, "", true, true, publishing); err != nil {
		return err
	}

	return nil
}

func (p *Publisher[T]) Close() error {
	var err error

	if exClose := p.exchange.Close(); err != nil {
		err = multierr.Append(err, exClose)
	}

	if connErr := p.connection.Close(); connErr != nil {
		err = multierr.Append(err, connErr)
	}

	return err
}
