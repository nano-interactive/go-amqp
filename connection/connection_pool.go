package connection

import (
	"context"
	"errors"
	"github.com/enriquebris/goconcurrentqueue"
	"go.uber.org/multierr"
)

type Pool struct {
	cfg   *Config
	queue *goconcurrentqueue.FixedFIFO
}

func NewPool(size int, cfg *Config) (*Pool, error) {
	queue := goconcurrentqueue.NewFixedFIFO(size)

	for i := 0; i < size; i++ {
		conn, err := New(cfg)

		if err != nil {
			return nil, err
		}

		// Should never fail
		if err := queue.Enqueue(conn); err != nil {
			return nil, err
		}
	}

	return &Pool{
		queue: queue,
		cfg:   cfg,
	}, nil
}

func (p *Pool) Get(ctx context.Context) (Connection, error) {
	val, err := p.queue.DequeueOrWaitForNextElementContext(ctx)

	if err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return nil, err
	}

	conn := val.(Connection)

	if conn.IsClosed() {
		conn, err = New(p.cfg)

		if err != nil {
			return nil, err
		}

		return conn, nil
	}

	return conn, nil
}

func (p *Pool) Release(conn Connection) error {
	var err error

	// No need to create new connection here
	if conn.IsClosed() {
		return nil
	}

	if err = p.queue.Enqueue(conn); err != nil {
		if w, ok := err.(*goconcurrentqueue.QueueError); ok && w.Code() == goconcurrentqueue.QueueErrorCodeFullCapacity {
			if err = conn.Close(); err != nil {
				return err
			}

			return nil
		}

		return err
	}

	return nil
}

func (p *Pool) Close() error {
	var mErr error
	for val, err := p.queue.Dequeue(); err != nil; val, err = p.queue.Dequeue() {
		conn := val.(Connection)

		if closeErr := conn.Close(); closeErr != nil {
			mErr = multierr.Append(mErr, closeErr)
		}
	}

	return mErr
}
