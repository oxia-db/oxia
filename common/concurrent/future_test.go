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

func assertFutureNotReady[T any](t *testing.T, f Future[T]) {
	t.Helper()

	done := make(chan bool, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	go func() {
		_, err := f.Wait(ctx)
		assert.ErrorIs(t, err, context.Canceled)
		done <- true
	}()

	select {
	case <-done:
		assert.Fail(t, "should have timed out")

	case <-time.After(100 * time.Millisecond):
		// OK
	}
}

func TestFuture(t *testing.T) {
	f := NewFuture[int]()
	assertFutureNotReady(t, f)

	f.Complete(5)
	res, err := f.Wait(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 5, res)

	f = NewFuture[int]()
	f.Fail(io.EOF)
	res, err = f.Wait(context.Background())
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, res)
}

func TestFuture_Cancel(t *testing.T) {
	f := NewFuture[int]()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)

	go func() {
		res, err := f.Wait(ctx)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 0, res)
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

func TestFuture_Timeout(t *testing.T) {
	f := NewFuture[int]()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan bool)

	go func() {
		res, err := f.Wait(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, 0, res)
		done <- true
	}()

	select {
	case <-done:
	// Ok
	case <-time.After(1 * time.Second):
		assert.Fail(t, "shouldn't have timed out")
	}
}
