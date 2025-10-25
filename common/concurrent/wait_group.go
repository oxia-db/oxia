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

package concurrent

import "context"

// WaitGroup is similar to sync.WaitGroup but adds 2 capabilities:
//  1. Returning an error if any operation fails
//  2. Accept a context to cancel the Wait
type WaitGroup interface {

	// Wait until all the parties in the group are either done or if there is any failure
	// You should only call wait once
	Wait(ctx context.Context) error

	// Done Signals that one party in the group is done
	Done()

	// Fail Signal that one party has failed in the operation
	Fail(err error)

	// Go Run the function in a go-routing and automaticatly mark as done or failed
	Go(f func() error)
}

type waitGroup struct {
	parties   int
	responses chan error
}

func NewWaitGroup(parties int) WaitGroup {
	return &waitGroup{
		parties:   parties,
		responses: make(chan error, parties),
	}
}

func (g *waitGroup) Go(f func() error) {
	go func() {
		if err := f(); err != nil {
			g.Fail(err)
		} else {
			g.Done()
		}
	}()
}

func (g *waitGroup) Wait(ctx context.Context) error {
	for i := 0; i < g.parties; i++ {
		select {
		case err := <-g.responses:
			if err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Everyone has completed successfully
	return nil
}

func (g *waitGroup) Done() {
	select {
	case g.responses <- nil:
	default:
	}
}

func (g *waitGroup) Fail(err error) {
	g.responses <- err
}
