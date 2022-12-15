package publisher

import (
	"context"
	"fmt"
	"sync"
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

	Publisher[T Message] struct {
		cancel     context.CancelFunc
		wg         *sync.WaitGroup
		conn       connection.Connection
		serializer serializer.Serializer[T]
		counter    atomic.Uint64
		publish    []chan amqp091.Publishing
	}
)

func New[T Message](ctx context.Context, conn connection.Connection, options ...Option[T]) (*Publisher[T], error) {
	cfg := Config[T]{
		serializer:        serializer.JsonSerializer[T]{},
		logger:            &amqp.EmptyLogger{},
		connectionTimeout: 1 * time.Second,
		publishers:        1,
		messageBuffering:  1,
	}

	var msg T

	for _, option := range options {
		option(&cfg)
	}

	publishers := make([]chan amqp091.Publishing, 0, cfg.publishers)
	errChs := make([]chan error, 0, cfg.publishers)

	setupErrChs := make([]chan error, 0, cfg.publishers)
	name := msg.GetExchangeName()

	wg := &sync.WaitGroup{}
	wg.Add(cfg.publishers)
	newCtx, cancel := context.WithCancel(ctx)

	for i := 0; i < cfg.publishers; i++ {
		setupErrCh := make(chan error, 1)
		setupErrChs = append(setupErrChs, setupErrCh)

		publish := make(chan amqp091.Publishing, cfg.messageBuffering)
		publishers = append(publishers, publish)
		//errCh := make(chan error, cfg.messageBuffering)
		//errChs = append(errChs, errCh)

		go func(setupErr chan<- error) {
			ch, err := conn.RawConnection().Channel()
			if err != nil {
				setupErr <- err
				return
			}

			err = ch.ExchangeDeclare(
				msg.GetExchangeName(),
				msg.GetExchangeType().String(),
				true,
				false,
				false,
				false,
				nil,
			)

			defer func() {
				close(publish)

				for pub := range publish {
					// TODO: Handle the error
					ch.PublishWithDeferredConfirmWithContext(
						ctx,
						name,
						"",
						true,
						false,
						pub,
					)
				}

				if !ch.IsClosed() {
					_ = ch.Close()
				}

				wg.Done()
			}()

			close(setupErr)

			for {
				select {
				case <-newCtx.Done():
					return
				case pub := <-publish:
					_, err := ch.PublishWithDeferredConfirmWithContext(
						newCtx,
						name,
						"",
						true,
						false,
						pub,
					)

					if err != nil {
						//errCh <- err
						continue
					}

					//errCh <- nil
				}
			}
		}(setupErrCh)
	}

	if err := mergeErrors(setupErrChs); err != nil {
		for _, errCh := range errChs {
			close(errCh)
		}

		for _, publisher := range publishers {
			close(publisher)
		}

		_ = conn.Close()

		cancel()
		wg.Wait()
		return nil, err
	}

	return &Publisher[T]{
		cancel:     cancel,
		wg:         wg,
		conn:       conn,
		serializer: cfg.serializer,
		publish:    publishers,
	}, nil
}

func (p *Publisher[T]) Publish(ctx context.Context, msg T) error {
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

	publish := p.publish[p.counter.Add(1)%uint64(len(p.publish))]

	select {
	case <-ctx.Done():
		return ctx.Err()
	case publish <- publishing:
		fmt.Println("Published")
	}

	// FIXME: Listening on err channel might not be good
	// FIXME: since this method can be used from multiple goroutines
	// FIXME: and error can be from transaction before and
	// FIXME: not from the current published message
	return nil
}

func (p *Publisher[T]) Close() error {
	p.cancel()
	p.wg.Wait()
	return p.conn.Close()
}

func mergeErrors(errs []chan error) error {
	var wg sync.WaitGroup
	wg.Add(len(errs))
	out := make(chan error, len(errs))
	for _, setupErr := range errs {
		go func(wg *sync.WaitGroup, setupErr chan error) {
			defer wg.Done()
			for err := range setupErr {
				out <- err
			}
		}(&wg, setupErr)
	}

	wg.Wait()
	close(out)
	return <-out
}
