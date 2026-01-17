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

package option

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestNewWatch verifies that a new Watch is properly initialized with the initial value and version 0.
func TestNewWatch(t *testing.T) {
	w := NewWatch("initial")

	value, version := w.Load()
	assert.Equal(t, "initial", value)
	assert.Equal(t, uint64(0), version)
}

func TestWatchLoad(t *testing.T) {
	w := NewWatch[int](42)

	value, version := w.Load()
	assert.Equal(t, 42, value)
	assert.Equal(t, uint64(0), version)
}

func TestWatchNotify(t *testing.T) {
	w := NewWatch("initial")

	w.Notify("updated")

	value, version := w.Load()
	assert.Equal(t, "updated", value)
	assert.Equal(t, uint64(1), version)
}

func TestWatchNotifyMultipleTimes(t *testing.T) {
	w := NewWatch[int](0)

	for i := 1; i <= 5; i++ {
		w.Notify(i * 10)
		value, version := w.Load()
		assert.Equal(t, i*10, value)
		assert.Equal(t, uint64(i), version)
	}
}

func TestWaitWithNewerVersion(t *testing.T) {
	w := NewWatch("initial")

	w.Notify("updated")

	value, version, err := w.Wait(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, "updated", value)
	assert.Equal(t, uint64(1), version)
}

func TestWaitWithSameVersion(t *testing.T) {
	w := NewWatch("initial")

	done := make(chan struct{})
	go func() {
		defer close(done)
		value, version, err := w.Wait(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, "updated", value)
		assert.Equal(t, uint64(1), version)
	}()

	time.Sleep(10 * time.Millisecond)
	w.Notify("updated")

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait didn't complete after notification")
	}
}

func TestWaitConcurrent(t *testing.T) {
	w := NewWatch[int](0)

	var wg sync.WaitGroup
	results := make([]struct {
		value   int
		version uint64
	}, 5)

	for i := 0; i < 5; i++ {
		fi := i
		wg.Go(func() {
			value, version, err := w.Wait(context.Background(), 0)
			assert.NoError(t, err)
			results[fi] = struct {
				value   int
				version uint64
			}{value: value, version: version}
		})
	}

	time.Sleep(10 * time.Millisecond)
	w.Notify(100)

	wg.Wait()

	for i := 0; i < 5; i++ {
		assert.Equal(t, 100, results[i].value)
		assert.Equal(t, uint64(1), results[i].version)
	}
}

func TestWaitMultipleNotification(t *testing.T) {
	w := NewWatch[int](0)

	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		value, version, err := w.Wait(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, 100, value)
		assert.Equal(t, uint64(1), version)
	}()

	time.Sleep(10 * time.Millisecond)
	w.Notify(100)

	select {
	case <-done1:
	case <-time.After(time.Second):
		t.Fatal("First wait didn't complete")
	}

	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		value, version, err := w.Wait(context.Background(), 1)
		assert.NoError(t, err)
		assert.Equal(t, 200, value)
		assert.Equal(t, uint64(2), version)
	}()

	time.Sleep(10 * time.Millisecond)
	w.Notify(200)

	select {
	case <-done2:
	case <-time.After(time.Second):
		t.Fatal("Second wait didn't complete")
	}
}

func TestWatchWithStruct(t *testing.T) {
	type TestStruct struct {
		Name string
		Age  int
	}

	initial := TestStruct{Name: "Alice", Age: 25}
	w := NewWatch(initial)

	value, version := w.Load()
	assert.Equal(t, initial, value)
	assert.Equal(t, uint64(0), version)

	updated := TestStruct{Name: "Bob", Age: 30}
	w.Notify(updated)

	value, version = w.Load()
	assert.Equal(t, updated, value)
	assert.Equal(t, uint64(1), version)
}

func TestWatchWithNilValues(t *testing.T) {
	w := NewWatch[*string](nil)

	value, version := w.Load()
	assert.Nil(t, value)
	assert.Equal(t, uint64(0), version)

	str := "test"
	w.Notify(&str)

	value, version = w.Load()
	assert.NotNil(t, value)
	assert.Equal(t, "test", *value)
	assert.Equal(t, uint64(1), version)
}

func TestWatchRapidNotifications(t *testing.T) {
	w := NewWatch[int](0)

	// Rapid successive notifications
	for i := 1; i <= 1000; i++ {
		w.Notify(i)
	}

	value, version := w.Load()
	assert.Equal(t, 1000, value)
	assert.Equal(t, uint64(1000), version)
}

func TestWatchConcurrentLoadAndNotify(t *testing.T) {
	tests := []struct {
		name                string
		numLoaders          int
		loadsPerLoader      int
		numNotifiers        int
		notifiesPerNotifier int
		minExpectedVersion  uint64
	}{
		{
			name:                "small_concurrent",
			numLoaders:          5,
			loadsPerLoader:      10,
			numNotifiers:        2,
			notifiesPerNotifier: 5,
			minExpectedVersion:  10,
		},
		{
			name:                "medium_concurrent",
			numLoaders:          10,
			loadsPerLoader:      100,
			numNotifiers:        5,
			notifiesPerNotifier: 10,
			minExpectedVersion:  50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWatch[int](0)
			var wg sync.WaitGroup

			// Concurrent loaders
			for i := 0; i < tt.numLoaders; i++ {
				wg.Go(func() {
					for j := 0; j < tt.loadsPerLoader; j++ {
						w.Load()
					}
				})
			}

			// Concurrent notifiers
			for i := 0; i < tt.numNotifiers; i++ {
				fi := i
				wg.Go(func() {
					for j := 0; j < tt.notifiesPerNotifier; j++ {
						w.Notify(fi*100 + j)
					}
				})
			}

			wg.Wait()

			_, finalVersion := w.Load()
			assert.GreaterOrEqual(t, finalVersion, tt.minExpectedVersion)
		})
	}
}

func TestWaitRaceCondition(t *testing.T) {
	w := NewWatch[int](0)

	var wg sync.WaitGroup
	done := make(chan bool, 100)

	// Start many waiters
	for i := 0; i < 100; i++ {
		wg.Go(func() {
			value, version, err := w.Wait(context.Background(), 0)
			assert.NoError(t, err)
			if value == 999 && version == 1 {
				done <- true
			}
		})
	}

	time.Sleep(10 * time.Millisecond)
	w.Notify(999)

	wg.Wait()

	// All waiters should have been notified
	assert.Len(t, done, 100)
}

func TestWatchVersionIncrement(t *testing.T) {
	w := NewWatch[string]("")

	assert.Equal(t, uint64(0), getVersion(w))

	w.Notify("v1")
	assert.Equal(t, uint64(1), getVersion(w))

	w.Notify("v2")
	assert.Equal(t, uint64(2), getVersion(w))

	w.Notify("v3")
	assert.Equal(t, uint64(3), getVersion(w))
}

func TestWaitContextCancellation(t *testing.T) {
	w := NewWatch[int](0)

	// Test cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, _, err := w.Wait(ctx, 1)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestWaitTimeout(t *testing.T) {
	w := NewWatch[int](0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, _, err := w.Wait(ctx, 1)
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func getVersion[T any](w *Watch[T]) uint64 {
	_, version := w.Load()
	return version
}
