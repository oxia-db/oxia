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

	"github.com/oxia-db/oxia/common/process"
)

const (
	StatusActive Status = 0
	StatusPaused Status = 1
	StatusClosed Status = 2
)

type Errors struct {
	Pause    error
	Shutdown error
}

type Status = int

type Actor[T any] struct {
	cx     context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	ers    Errors

	queue  []T
	status Status

	handler func([]T)
}

func New[T any](ctx context.Context, name string, handler func([]T), ers Errors) (*Actor[T], error) {
	if handler == nil {
		return nil, errors.New("process function must not be nil")
	}
	actorCtx, actorCtxCancelFunc := context.WithCancel(ctx)
	a := &Actor[T]{
		cx:      actorCtx,
		cancel:  actorCtxCancelFunc,
		wg:      sync.WaitGroup{},
		mu:      sync.Mutex{},
		ers:     ers,
		queue:   []T{},
		status:  StatusActive,
		handler: handler,
	}

	a.wg.Go(func() {
		process.DoWithLabels(ctx, map[string]string{
			"oxia": fmt.Sprintf("actor-%s", name),
		}, a.run)
	})
	return a, nil
}

func (a *Actor[T]) Send(item T) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch a.status {
	case StatusPaused:
		return a.ers.Pause
	case StatusClosed:
		return a.ers.Shutdown
	}
	a.queue = append(a.queue, item)
	return nil
}

func (a *Actor[T]) Pause() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.status == StatusClosed {
		return a.ers.Shutdown
	}
	a.status = StatusPaused
	return nil
}

func (a *Actor[T]) Resume() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.status == StatusClosed {
		return a.ers.Shutdown
	}
	a.status = StatusActive
	return nil
}

func (a *Actor[T]) Close() error {
	a.mu.Lock()
	a.status = StatusClosed
	a.mu.Unlock()
	a.cancel()
	a.wg.Wait()
	return nil
}

func (a *Actor[T]) run() {
	for {
		select {
		case <-a.cx.Done():
			a.handle()
			return
		default:
			for {
				if skip := a.handle(); skip {
					break
				}
			}
		}
	}
}

func (a *Actor[T]) handle() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	batch := a.queue
	a.queue = make([]T, 0)
	if len(batch) == 0 {
		return true
	}
	a.handler(batch)
	return false
}
