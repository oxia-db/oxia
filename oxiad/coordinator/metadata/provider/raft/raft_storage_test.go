// Copyright 2023-2026 The Oxia Authors
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

package raft

import (
	"math"
	"testing"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *kvRaftStore {
	t.Helper()
	store, err := newKVRaftStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	return store
}

func storeLogs(t *testing.T, store *kvRaftStore, indexes ...uint64) {
	t.Helper()
	logs := make([]*hashicorpraft.Log, len(indexes))
	for i, index := range indexes {
		logs[i] = &hashicorpraft.Log{Index: index, Term: 1, Data: []byte("entry")}
	}
	require.NoError(t, store.StoreLogs(logs))
}

func TestKVRaftStoreLogRoundTrip(t *testing.T) {
	store := newTestStore(t)

	first, err := store.FirstIndex()
	require.NoError(t, err)
	assert.EqualValues(t, 0, first)

	storeLogs(t, store, 1, 2, 3, 4, 5)

	first, err = store.FirstIndex()
	require.NoError(t, err)
	assert.EqualValues(t, 1, first)

	last, err := store.LastIndex()
	require.NoError(t, err)
	assert.EqualValues(t, 5, last)

	log := &hashicorpraft.Log{}
	require.NoError(t, store.GetLog(3, log))
	assert.EqualValues(t, 3, log.Index)
	assert.Equal(t, []byte("entry"), log.Data)
}

func TestKVRaftStoreDeleteRange(t *testing.T) {
	store := newTestStore(t)
	storeLogs(t, store, 1, 2, 3, 4, 5)

	require.NoError(t, store.DeleteRange(1, 3))

	first, err := store.FirstIndex()
	require.NoError(t, err)
	assert.EqualValues(t, 4, first)
}

// maxInclusive+1 overflows when maxInclusive is MaxUint64, inverting the
// range into a no-op that silently deletes nothing.
func TestKVRaftStoreDeleteRangeMaxUint64(t *testing.T) {
	store := newTestStore(t)
	storeLogs(t, store, 1, 2, 3)

	require.NoError(t, store.DeleteRange(0, math.MaxUint64))

	first, err := store.FirstIndex()
	require.NoError(t, err)
	assert.EqualValues(t, 0, first)
	last, err := store.LastIndex()
	require.NoError(t, err)
	assert.EqualValues(t, 0, last)
}

func TestKVRaftStoreStableStore(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.Set([]byte("key"), []byte("value")))
	value, err := store.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)

	require.NoError(t, store.SetUint64([]byte("counter"), 42))
	n, err := store.GetUint64([]byte("counter"))
	require.NoError(t, err)
	assert.EqualValues(t, 42, n)
}
