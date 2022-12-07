package publisher

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nano-interactive/go-amqp/connection"
	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/multierr"
)

type (
	exchange struct {
		amqp    []ex
		ctx     context.Context
		counter atomic.Uint64
	}

	ex struct {
		*sync.RWMutex
		ready   *atomic.Bool
		channel *amqp091.Channel
		conn    connection.Connection
	}
)

func recreate(ex *ex, conn connection.Connection, errCh chan *amqp091.Error) {
	var err error

	for amqpErr := range errCh {
		if amqpErr.Code != amqp091.ChannelError {
			continue
		}

		select {
		case _, more := <-errCh:
			if more {
				close(errCh)
			}
		default:
			close(errCh)
		}

		ex.channel, err = conn.RawConnection().Channel()

		if err != nil {
			ex.Unlock()
			ex.ready.Store(false)
			return
		}

		errCh := ex.channel.NotifyClose(make(chan *amqp091.Error, 1))
		go recreate(ex, conn, errCh)

		ex.Unlock()
		ex.ready.Store(true)
		return
	}
}

func newEx(conn connection.Connection) (ex, error) {
	ch, err := conn.RawConnection().Channel()
	if err != nil {
		return ex{}, err
	}

	ready := &atomic.Bool{}
	ready.Store(true)

	ex := ex{
		RWMutex: &sync.RWMutex{},
		channel: ch,
		ready:   ready,
	}

	errCh := ch.NotifyClose(make(chan *amqp091.Error, 1))

	go recreate(&ex, conn, errCh)

	return ex, nil
}

func (e *ex) Close() error {
	e.ready.Store(false)
	e.RLock()
	defer e.RUnlock()

	if e.channel.IsClosed() {
		return nil
	}

	return e.channel.Close()
}

func (e *exchange) Close() (err error) {
	for _, ex := range e.amqp {
		err = multierr.Append(err, ex.Close())
	}
	return err
}

func newExchange(conn connection.Connection, ctx context.Context, chCount uint32) (*exchange, error) {
	chs := make([]ex, chCount)

	var err error
	for i := uint32(0); i < chCount; i++ {
		chs[i], err = newEx(conn)
		if err != nil {
			return nil, err
		}
	}

	ex := &exchange{
		ctx:     ctx,
		amqp:    chs,
		counter: atomic.Uint64{},
	}

	ex.counter.Store(0)

	return ex, nil
}

func (e *exchange) GetChannel() (*ex, error) {
	var val *ex
	val.ready.Store(false)

	for !val.ready.Load() {
		if val.conn.IsClosed() {
			return nil, fmt.Errorf("connection is closed")
		}

		select {
		case <-e.ctx.Done():
			return nil, context.Canceled
		default:
			val = &e.amqp[e.counter.Add(1)%uint64(len(e.amqp))]
		}
	}

	return val, nil
}
