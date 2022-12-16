package publisher

import (
	"context"
	"sync"

	"github.com/nano-interactive/go-amqp/connection"
)

func worker[T Message](
	ctx context.Context,
	conn connection.Connection,
	wg *sync.WaitGroup,
	setupErr chan<- error,
	publish chan publishing,
	workerExit chan struct{},
) {
	defer func() {

	}()

	var msg T

	exchangeName := msg.GetExchangeName()

	ch, err := conn.RawConnection().Channel()
	if err != nil {
		setupErr <- err
		close(setupErr)
		return
	}

	err = ch.ExchangeDeclare(
		exchangeName,
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
			_, err = ch.PublishWithDeferredConfirmWithContext(
				ctx,
				exchangeName,
				"",
				true,
				false,
				pub.Publishing,
			)

			if err != nil && pub.errCb != nil {
				pub.errCb(err)
			}
		}

		if !ch.IsClosed() {
			_ = ch.Close()
		}

		contextErr := ctx.Err()

		if contextErr != context.Canceled && contextErr != context.DeadlineExceeded {
			workerExit <- struct{}{}
		}
		wg.Done()
	}()

	close(setupErr)

	for {
		select {
		case <-ctx.Done():
			return
		case pub := <-publish:
			_, err := ch.PublishWithDeferredConfirmWithContext(
				ctx,
				exchangeName,
				"",
				true,
				false,
				pub.Publishing,
			)

			if err != nil && pub.errCb != nil {
				pub.errCb(err)
				continue
			}
		}
	}
}
