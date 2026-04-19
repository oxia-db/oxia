// Copyright 2023-2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/common/process"
)

var (
	ErrPaused       = errors.New("paused")
	ErrShuttingDown = errors.New("shutting down")
)

type Status int

const (
	StatusActive Status = iota
	StatusPaused
	StatusClosed
)

type Actor[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
	mu sync.Mutex

	queue  []T
	status Status

	handler func([]T)
	notify  chan struct{}
}

func New[T any](ctx context.Context, name string, handler func([]T), status Status) (*Actor[T], error) {
	actorCtx, cancel := context.WithCancel(ctx)
	if name == "" {
		name = "actor"
	}

	a := &Actor[T]{
		ctx:     actorCtx,
		cancel:  cancel,
		queue:   make([]T, 0),
		status:  status,
		handler: handler,
		notify:  make(chan struct{}, 1),
	}

	a.wg.Go(func() {
		process.DoWithLabels(actorCtx, map[string]string{
			"oxia": fmt.Sprintf("actor-%s", name),
		}, a.run)
	})

	return a, nil
}

func (a *Actor[T]) Send(item T) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch a.status {
	case StatusActive:
	case StatusPaused:
		return ErrPaused
	case StatusClosed:
		return ErrShuttingDown
	default:
		panic("unknown actor status")
	}

	a.queue = append(a.queue, item)
	channel.PushNoBlock(a.notify, struct{}{})
	return nil
}

func (a *Actor[T]) Pause() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.status == StatusClosed {
		return ErrShuttingDown
	}

	a.status = StatusPaused
	return nil
}

func (a *Actor[T]) Resume() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.status == StatusClosed {
		return ErrShuttingDown
	}

	a.status = StatusActive
	if len(a.queue) > 0 {
		channel.PushNoBlock(a.notify, struct{}{})
	}
	return nil
}

func (a *Actor[T]) Close() error {
	a.mu.Lock()
	if a.status == StatusClosed {
		a.mu.Unlock()
		a.wg.Wait()
		return nil
	}
	a.status = StatusClosed
	a.mu.Unlock()

	a.cancel()
	a.wg.Wait()
	return nil
}

func (a *Actor[T]) run() {
	for {
		select {
		case <-a.ctx.Done():
			a.mu.Lock()
			a.status = StatusClosed
			a.mu.Unlock()
			return
		case <-a.notify:
		}

		for {
			batch, done := a.take()
			if done {
				return
			}
			if len(batch) == 0 {
				break
			}
			a.handler(batch)
		}
	}
}

func (a *Actor[T]) take() ([]T, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.status == StatusClosed {
		return nil, true
	}
	if len(a.queue) == 0 {
		return nil, false
	}

	batch := a.queue
	a.queue = make([]T, 0)
	return batch, false
}
