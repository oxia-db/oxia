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
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestBatchOnce_FlushByCounter(t *testing.T) {
	queue := make(chan int, 20)
	finish := make(chan error, 1)

	streamOnceCb := NewBatchStreamOnce[int](3, 2048, func(t int) int {
		return t
	}, func(container []int) error {
		for _, el := range container {
			queue <- el
		}
		return nil
	}, func(err error) {
		finish <- err
		close(finish)
		close(queue)
	})

	assert.NoError(t, streamOnceCb.OnNext(1))
	assert.NoError(t, streamOnceCb.OnNext(2))
	assert.NoError(t, streamOnceCb.OnNext(3))

	// expect flush
	for i := 1; i <= 3; i++ {
		el := <-queue
		assert.Equal(t, i, el)
	}
	assert.Empty(t, queue)

	assert.NoError(t, streamOnceCb.OnNext(4))
	assert.NoError(t, streamOnceCb.OnNext(5))
	assert.NoError(t, streamOnceCb.OnNext(6))

	// expect flush
	for i := 4; i <= 6; i++ {
		el := <-queue
		assert.Equal(t, i, el)
	}
	assert.Empty(t, queue)

	assert.NoError(t, streamOnceCb.OnNext(7))
	assert.NoError(t, streamOnceCb.OnNext(8))
	streamOnceCb.OnComplete(nil)

	// expect final flush
	for i := 7; i <= 8; i++ {
		el := <-queue
		assert.Equal(t, i, el)
	}
	assert.Empty(t, queue)

	_, more := <-queue
	assert.False(t, more)
	f := <-finish
	assert.Nil(t, f)
	_, more = <-finish
	assert.False(t, more)
}

func TestBatchOnce_FlushByBytes(t *testing.T) {
	queue := make(chan int, 20)
	finish := make(chan error, 1)

	streamOnceCb := NewBatchStreamOnce[int](10, 2048, func(t int) int {
		return t
	}, func(container []int) error {
		for _, el := range container {
			queue <- el
		}
		return nil
	}, func(err error) {
		finish <- err
		close(finish)
		close(queue)
	})

	assert.NoError(t, streamOnceCb.OnNext(1024))
	assert.NoError(t, streamOnceCb.OnNext(1024))
	assert.NoError(t, streamOnceCb.OnNext(1024))

	// expect flush
	for i := 0; i < 2; i++ {
		el := <-queue
		assert.Equal(t, el, 1024)
	}
	assert.Empty(t, queue)

	assert.NoError(t, streamOnceCb.OnNext(1024))
	assert.NoError(t, streamOnceCb.OnNext(1024))
	assert.NoError(t, streamOnceCb.OnNext(1024))

	// expect flush
	for i := 0; i < 4; i++ {
		el := <-queue
		assert.Equal(t, el, 1024)
	}
	assert.Empty(t, queue)

	assert.NoError(t, streamOnceCb.OnNext(1024))
	streamOnceCb.OnComplete(nil)

	// expect final flush
	el := <-queue
	assert.Equal(t, 1024, el)
	assert.Empty(t, queue)

	_, more := <-queue
	assert.False(t, more)
	f := <-finish
	assert.Nil(t, f)
	_, more = <-finish
	assert.False(t, more)
}

func TestBatchOnce_Error(t *testing.T) {
	queue := make(chan int, 20)
	finish := make(chan error, 1)

	streamOnceCb := NewBatchStreamOnce[int](10, 2048, func(t int) int {
		return t
	}, func(container []int) error {
		for _, el := range container {
			queue <- el
		}
		return nil
	}, func(err error) {
		finish <- err
		close(finish)
		close(queue)
	})

	assert.NoError(t, streamOnceCb.OnNext(1024))
	streamOnceCb.OnComplete(errors.New("test"))

	_, more := <-queue
	assert.False(t, more)
	f := <-finish
	assert.Error(t, f)
	_, more = <-finish
	assert.False(t, more)
}
