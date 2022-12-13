package publisher

import (
	"context"
	"github.com/nano-interactive/go-amqp/connection"
	"github.com/rabbitmq/amqp091-go"
)

func recreate[T Message](base context.Context, publisher *Publisher[T], cfg Config[T], pool *connection.Pool, errCh chan *amqp091.Error) {
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

		newCtx, cancel := context.WithTimeout(base, cfg.connectionTimeout)
		conn, err := pool.Get(newCtx)

		if err != nil {
			cancel()
			publisher.errCh <- err
			return
		}

		pool.Release(conn)
		publisher.m.Lock()

		publisher.channel, err = conn.RawConnection().Channel()

		if err != nil {
			cancel()
			pool.Release(conn)
			publisher.errCh <- err
			publisher.m.Unlock()
			return
		}

		newErrCh := publisher.channel.NotifyClose(make(chan *amqp091.Error, 1))
		go recreate(base, publisher, cfg, pool, newErrCh)
		cancel()
		pool.Release(conn)
		publisher.m.Unlock()
		return
	}
}
