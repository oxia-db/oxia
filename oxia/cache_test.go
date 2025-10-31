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

package oxia

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/concurrent"

	"github.com/oxia-db/oxia/node"
)

type testStruct struct {
	A string `json:"a"`
	B int    `json:"b"`
}

func newKey() string {
	return fmt.Sprintf("/my-key-%d", time.Now().Nanosecond())
}

func TestCache_Empty(t *testing.T) {
	standaloneServer, err := node.NewStandalone(node.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client, err := NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	cache, err := NewCache[testStruct](client, json.Marshal, json.Unmarshal)
	assert.NoError(t, err)

	value, version, err := cache.Get(context.Background(), "/non-existing-key")
	assert.ErrorIs(t, ErrKeyNotFound, err)
	assert.Equal(t, Version{}, version)
	assert.Equal(t, testStruct{}, value)

	value, version, err = cache.Get(context.Background(), "/non-existing-key/child")
	assert.ErrorIs(t, ErrKeyNotFound, err)
	assert.Equal(t, Version{}, version)
	assert.Equal(t, testStruct{}, value)

	err = cache.Delete(context.Background(), "/non-existing-key")
	assert.ErrorIs(t, ErrKeyNotFound, err)

	err = cache.Delete(context.Background(), "/non-existing-key/child")
	assert.ErrorIs(t, ErrKeyNotFound, err)

	assert.NoError(t, cache.Close())
	assert.NoError(t, client.Close())
}

func TestCache_InsertionDeletion(t *testing.T) {
	standaloneServer, err := node.NewStandalone(node.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client, err := NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	cache, err := NewCache[testStruct](client, json.Marshal, json.Unmarshal)
	assert.NoError(t, err)

	k1 := newKey()
	v1 := testStruct{"hello", 1}
	_, version1, err := cache.Put(context.Background(), k1, v1, ExpectedRecordNotExists())
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version1.ModificationsCount)

	v2 := testStruct{"hello", 2}
	_, version2, err := cache.Put(context.Background(), k1, v2, ExpectedRecordNotExists())
	assert.ErrorIs(t, err, ErrUnexpectedVersionId)
	assert.Equal(t, Version{}, version2)

	value, version, err := cache.Get(context.Background(), k1)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version.ModificationsCount)
	assert.Equal(t, v1, value)

	err = cache.ReadModifyUpdate(context.Background(), k1, func(existingValue Optional[testStruct]) (testStruct, error) {
		return testStruct{
			A: existingValue.MustGet().A,
			B: 3,
		}, nil
	})
	assert.NoError(t, err)

	value, version, err = cache.Get(context.Background(), k1)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, version.ModificationsCount)
	assert.Equal(t, testStruct{"hello", 3}, value)

	err = cache.ReadModifyUpdate(context.Background(), k1, func(existingValue Optional[testStruct]) (testStruct, error) {
		ev, ok := existingValue.Get()
		assert.True(t, ok)
		return testStruct{
			A: ev.A,
			B: 4,
		}, nil
	})
	assert.NoError(t, err)

	value, version, err = cache.Get(context.Background(), k1)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, version.ModificationsCount)
	assert.Equal(t, testStruct{"hello", 4}, value)

	assert.NoError(t, cache.Close())
	assert.NoError(t, client.Close())
}

func TestCache_PutOutSideTheCache(t *testing.T) {
	standaloneServer, err := node.NewStandalone(node.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client, err := NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	cache, err := NewCache[testStruct](client, json.Marshal, json.Unmarshal)
	assert.NoError(t, err)

	k1 := newKey()
	v1 := testStruct{"hello", 1}
	data, err := json.Marshal(v1)
	assert.NoError(t, err)
	_, version1, err := client.Put(context.Background(), k1, data, ExpectedRecordNotExists())
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version1.ModificationsCount)

	value, version, err := cache.Get(context.Background(), k1)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version.ModificationsCount)
	assert.Equal(t, v1, value)
}

func TestCache_DeserializationFailure(t *testing.T) {
	standaloneServer, err := node.NewStandalone(node.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client, err := NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	cache, err := NewCache[testStruct](client, json.Marshal, json.Unmarshal)
	assert.NoError(t, err)

	k1 := newKey()
	_, _, err = client.Put(context.Background(), k1, []byte("invalid json"))
	assert.NoError(t, err)

	value, version, err := cache.Get(context.Background(), k1)
	assert.Error(t, err)
	assert.Equal(t, Version{}, version)
	assert.Equal(t, testStruct{}, value)
}

func TestCache_ConcurrentUpdate(t *testing.T) {
	standaloneServer, err := node.NewStandalone(node.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client1, err := NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	client2, err := NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	cache1, err := NewCache[testStruct](client1, json.Marshal, json.Unmarshal)
	assert.NoError(t, err)

	cache2, err := NewCache[testStruct](client2, json.Marshal, json.Unmarshal)
	assert.NoError(t, err)

	k1 := newKey()
	v1 := testStruct{"hello", 1}
	_, version1, err := cache1.Put(context.Background(), k1, v1, ExpectedRecordNotExists())
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version1.ModificationsCount)

	wg := concurrent.NewWaitGroup(2)
	wg1 := concurrent.NewWaitGroup(1)
	wg2 := concurrent.NewWaitGroup(1)
	wgResult := concurrent.NewWaitGroup(2)
	var isFirstTime atomic.Bool
	isFirstTime.Store(true)

	go func() {
		err := cache1.ReadModifyUpdate(context.Background(), k1, func(existingValue Optional[testStruct]) (testStruct, error) {
			if isFirstTime.Load() {
				wg.Done()
				_ = wg1.Wait(context.Background())
			}
			ev := existingValue.MustGet()
			return testStruct{ev.A, ev.B + 1}, nil
		})
		assert.NoError(t, err)
		wgResult.Done()
	}()

	go func() {
		err := cache2.ReadModifyUpdate(context.Background(), k1, func(existingValue Optional[testStruct]) (testStruct, error) {
			if isFirstTime.Load() {
				wg.Done()
				_ = wg2.Wait(context.Background())
			}
			ev := existingValue.MustGet()
			return testStruct{ev.A, ev.B + 1}, nil
		})

		assert.NoError(t, err)
		wgResult.Done()
	}()

	// Wait for both go-routines to read the existing version
	_ = wg.Wait(context.Background())

	// Unblock both of them to and cause the conflict
	isFirstTime.Store(false)
	wg1.Done()
	wg2.Done()

	// Wait for both operations to complete
	_ = wgResult.Wait(context.Background())

	value, version, err := cache1.Get(context.Background(), k1)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, version.ModificationsCount)
	assert.Equal(t, testStruct{"hello", 3}, value)

	value, version, err = cache2.Get(context.Background(), k1)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, version.ModificationsCount)
	assert.Equal(t, testStruct{"hello", 3}, value)

	assert.NoError(t, cache1.Close())
	assert.NoError(t, client1.Close())

	assert.NoError(t, cache2.Close())
	assert.NoError(t, client2.Close())
}
