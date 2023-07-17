package publisher

import (
	"sync"
	"sync/atomic"
)

type guardLock struct {
	mu     sync.RWMutex
	locked atomic.Bool
}

func (g *guardLock) Lock() {
	if g.locked.Load() {
		return
	}

	g.mu.Lock()
	g.locked.Store(true)
}

func (g *guardLock) Unlock() {
	if !g.locked.Load() {
		return
	}

	g.mu.Unlock()
	g.locked.Store(false)
}

func (g *guardLock) RLock() {
	g.mu.RLock()
}

func (g *guardLock) RUnlock() {
	g.mu.RUnlock()
}
