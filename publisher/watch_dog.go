package publisher

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/nano-interactive/go-amqp/connection"
)

func watchdog[T Message](
	ctx context.Context,
	conn connection.Connection,
	wg *sync.WaitGroup,
	publish chan publishing,
	workerExit chan struct{},
	ready *atomic.Bool,
) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-workerExit:
			ready.Store(false)
			setupErrCh := make(chan error, 1)
			go worker[T](ctx, conn, wg, setupErrCh, publish, workerExit)
			if err := <-setupErrCh; err != nil {
				continue
			}

			ready.Store(true)
		}
	}
}
