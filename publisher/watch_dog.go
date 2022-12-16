package publisher

import (
	"context"
	"github.com/nano-interactive/go-amqp/connection"
	"sync"
)

func watchdog[T Message](
	ctx context.Context,
	conn connection.Connection,
	wg *sync.WaitGroup,
	publish chan publishing,
	workerExit chan struct{},
) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-workerExit:
			setupErrCh := make(chan error, 1)
			go worker[T](ctx, conn, wg, setupErrCh, publish, workerExit)
			if err := <-setupErrCh; err != nil {
				continue
			}
		}
	}
}
