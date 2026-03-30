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

// Copyright 2023-2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package concurrent

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/entity"
)

func Test_Once_Complete_Concurrent(t *testing.T) {
	callbackCounter := atomic.Int32{}
	onceCallback := NewOnce[any](
		func(t any) {
			callbackCounter.Add(1)
		},
		func(err error) {
			callbackCounter.Add(1)
		})

	group := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		group.Add(1)
		go func() {
			if i%2 == 0 {
				onceCallback.OnComplete(nil)
			} else {
				onceCallback.OnCompleteError(errors.New("error"))
			}
			group.Done()
		}()
	}

	group.Wait()
	assert.Equal(t, int32(1), callbackCounter.Load())
}

func Test_Once_Complete(t *testing.T) {
	var callbackError error
	var callbackValue int32

	onceCallback := NewOnce[int32](
		func(t int32) {
			callbackValue = t
		},
		func(err error) {
			callbackError = err
		})

	onceCallback.OnComplete(1)

	assert.Nil(t, callbackError)
	assert.Equal(t, int32(1), callbackValue)
}

func Test_Once_Complete_Error(t *testing.T) {
	var callbackError error
	var callbackValue *int32

	onceCallback := NewOnce[int32](
		func(t int32) {
			callbackValue = &t
		},
		func(err error) {
			callbackError = err
		})

	e1 := errors.New("error")
	onceCallback.OnCompleteError(e1)

	assert.Equal(t, e1, callbackError)
	assert.Nil(t, callbackValue)
}

func Test_Once_Stream_CompleteConcurrent(t *testing.T) {
	callbackCounter := atomic.Int32{}
	onceCallback := NewStreamOnce[any](func(a any) error {
		return nil
	}, func(err error) {
		callbackCounter.Add(1)
	},
	)

	group := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		group.Add(1)
		go func() {
			if i%2 == 0 {
				onceCallback.OnComplete(nil)
			}
			group.Done()
		}()
	}
	group.Wait()
	assert.Equal(t, int32(1), callbackCounter.Load())
}

func Test_Once_Stream_ReadFromStreamCallback(t *testing.T) {
	ch := make(chan *entity.TWithError[int], 10)
	callback := ReadFromStreamCallback(ch)

	assert.NoError(t, callback.OnNext(1))
	assert.NoError(t, callback.OnNext(2))
	assert.NoError(t, callback.OnNext(3))
	callback.OnComplete(nil)

	v := <-ch
	assert.Equal(t, 1, v.T)
	v = <-ch
	assert.Equal(t, 2, v.T)
	v = <-ch
	assert.Equal(t, 3, v.T)

	_, more := <-ch
	assert.False(t, more)
}

func Test_Once_Stream_ReadFromStreamCallback_Error(t *testing.T) {
	ch := make(chan *entity.TWithError[int], 10)
	callback := ReadFromStreamCallback(ch)

	assert.NoError(t, callback.OnNext(1))
	assert.NoError(t, callback.OnNext(2))
	assert.NoError(t, callback.OnNext(3))
	callback.OnComplete(errors.New("error"))

	v := <-ch
	assert.Equal(t, 1, v.T)
	v = <-ch
	assert.Equal(t, 2, v.T)
	v = <-ch
	assert.Equal(t, 3, v.T)
	v = <-ch
	assert.Error(t, v.Err)

	_, more := <-ch
	assert.False(t, more)
}
