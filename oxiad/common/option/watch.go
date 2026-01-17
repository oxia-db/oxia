package option

import (
	"context"
	"sync"
	"sync/atomic"
)

type ValueWithVersion[T any] struct {
	v   T
	ver uint64
}

// Watch provides a generic watch primitive for observing value changes
type Watch[T any] struct {
	mu     sync.RWMutex
	v      atomic.Pointer[ValueWithVersion[T]] // current value with version
	notify chan struct{}                       // notification channel for waiters
}

func (w *Watch[T]) Load() (T, uint64) {
	snapshot := w.v.Load()
	return snapshot.v, snapshot.ver
}

func (w *Watch[T]) Wait(ctx context.Context, waitVer uint64) (T, uint64, error) {
	snapshot := w.v.Load()
	if snapshot.ver > waitVer {
		return snapshot.v, snapshot.ver, nil
	}

	w.mu.RLock()
	notify := w.notify
	w.mu.RUnlock()
	select {
	case <-ctx.Done():
		notified := w.v.Load()
		return notified.v, notified.ver, ctx.Err()
	case <-notify:
		notified := w.v.Load()
		return notified.v, notified.ver, nil
	}
}

func (w *Watch[T]) Notify(value T) {
	w.mu.Lock()
	previousNotify := w.notify
	entity := w.v.Load()
	w.v.Store(&ValueWithVersion[T]{
		v:   value,
		ver: entity.ver + 1,
	})
	w.notify = make(chan struct{})
	w.mu.Unlock()

	close(previousNotify)
}

func NewWatch[T any](init T) *Watch[T] {
	w := Watch[T]{
		notify: make(chan struct{}),
		v:      atomic.Pointer[ValueWithVersion[T]]{},
	}
	w.v.Store(&ValueWithVersion[T]{
		v:   init,
		ver: 0,
	})
	return &w
}
