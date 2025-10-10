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

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func assertNotReady(t *testing.T, wg WaitGroup) {
	t.Helper()

	done := make(chan bool, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	go func() {
		assert.ErrorIs(t, wg.Wait(ctx), context.Canceled)
		done <- true
	}()

	select {
	case <-done:
		assert.Fail(t, "should have timed out")

	case <-time.After(100 * time.Millisecond):
		// OK
	}
}

func TestWaitGroup(t *testing.T) {
	wg := NewWaitGroup(3)
	assertNotReady(t, wg)

	wg = NewWaitGroup(1)
	assertNotReady(t, wg)
	wg.Done()
	assert.NoError(t, wg.Wait(context.Background()))

	wg = NewWaitGroup(3)
	wg.Done()
	wg.Done()
	assertNotReady(t, wg)

	wg = NewWaitGroup(3)
	assertNotReady(t, wg)
	wg.Done()
	wg.Done()
	wg.Done()
	assert.NoError(t, wg.Wait(context.Background()))

	wg = NewWaitGroup(3)
	wg.Fail(io.EOF)
	assert.ErrorIs(t, wg.Wait(context.Background()), io.EOF)

	wg = NewWaitGroup(3)
	wg.Done()
	wg.Fail(io.EOF)
	assert.ErrorIs(t, wg.Wait(context.Background()), io.EOF)
}

func TestWaitGroup_Cancel(t *testing.T) {
	wg := NewWaitGroup(3)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)

	go func() {
		assert.ErrorIs(t, wg.Wait(ctx), context.Canceled)
		done <- true
	}()

	cancel()
	select {
	case <-done:
	// Ok
	case <-time.After(1 * time.Second):
		assert.Fail(t, "shouldn't have timed out")
	}
}

func TestWaitGroup_Timeout(t *testing.T) {
	wg := NewWaitGroup(3)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan bool)

	go func() {
		assert.ErrorIs(t, wg.Wait(ctx), context.DeadlineExceeded)
		done <- true
	}()

	select {
	case <-done:
	// Ok
	case <-time.After(1 * time.Second):
		assert.Fail(t, "shouldn't have timed out")
	}
}
