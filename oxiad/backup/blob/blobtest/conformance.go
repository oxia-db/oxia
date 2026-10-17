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

// Package blobtest holds the conformance tests of the blob stores, which every
// implementation runs.
package blobtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/backup/blob"
)

// RunConformance runs the behaviors every blob.Store must have against the
// store that newStore returns, which must be empty.
func RunConformance(t *testing.T, newStore func(t *testing.T) blob.Store) {
	t.Helper()
	t.Run("GetReturnsNotFoundForMissingKey", func(t *testing.T) {
		store := newStore(t)
		_, err := store.Get(t.Context(), "missing")
		assert.ErrorIs(t, err, blob.ErrNotFound)
	})
	t.Run("PutThenGetReturnsContent", func(t *testing.T) {
		store := newStore(t)
		content := bytes.Repeat([]byte("abcdefgh"), 100_000)
		require.NoError(t, store.Put(t.Context(), "dir/file", bytes.NewReader(content)))
		assert.Equal(t, content, get(t, store, "dir/file"))
	})
	t.Run("PutReplacesExistingObject", func(t *testing.T) {
		store := newStore(t)
		require.NoError(t, store.Put(t.Context(), "k", strings.NewReader("one")))
		require.NoError(t, store.Put(t.Context(), "k", strings.NewReader("two")))
		assert.Equal(t, "two", string(get(t, store, "k")))
	})
	t.Run("PutStoresEmptyObject", func(t *testing.T) {
		store := newStore(t)
		require.NoError(t, store.Put(t.Context(), "empty", strings.NewReader("")))
		assert.Empty(t, get(t, store, "empty"))
		objects := list(t, store, "")
		require.Len(t, objects, 1)
		assert.Equal(t, int64(0), objects[0].Size)
	})
	t.Run("ListReturnsObjectsUnderPrefixInOrder", func(t *testing.T) {
		store := newStore(t)
		for _, key := range []string{"a/2", "a/1", "b/1", "a/x/3"} {
			require.NoError(t, store.Put(t.Context(), key, strings.NewReader(key)))
		}
		objects := list(t, store, "a/")
		keys := make([]string, 0, len(objects))
		for _, object := range objects {
			keys = append(keys, object.Key)
			assert.Equal(t, int64(len(object.Key)), object.Size)
			assert.False(t, object.LastModified.IsZero())
		}
		assert.Equal(t, []string{"a/1", "a/2", "a/x/3"}, keys)
	})
	t.Run("ListStopsAtVisitError", func(t *testing.T) {
		store := newStore(t)
		for i := range 5 {
			require.NoError(t, store.Put(t.Context(), fmt.Sprintf("k%d", i), strings.NewReader("v")))
		}
		stop := errors.New("stop")
		visited := 0
		err := store.List(t.Context(), "", func(blob.Object) error {
			visited++
			if visited == 2 {
				return stop
			}
			return nil
		})
		assert.ErrorIs(t, err, stop)
		assert.Equal(t, 2, visited)
	})
	t.Run("ListPaginatesLargeListing", func(t *testing.T) {
		store := newStore(t)
		const count = 1200
		for i := range count {
			require.NoError(t, store.Put(t.Context(), fmt.Sprintf("p/%05d", i), strings.NewReader("")))
		}
		assert.Len(t, list(t, store, "p/"), count)
	})
	t.Run("ListDirsReturnsNextSegments", func(t *testing.T) {
		store := newStore(t)
		for _, key := range []string{"ns/shard-1/a", "ns/shard-1/b/c", "ns/shard-2/a", "ns/top", "other/x"} {
			require.NoError(t, store.Put(t.Context(), key, strings.NewReader("")))
		}
		dirs, err := store.ListDirs(t.Context(), "ns/")
		require.NoError(t, err)
		assert.Equal(t, []string{"ns/shard-1/", "ns/shard-2/"}, dirs)
	})
	t.Run("DeleteRemovesObjectsAndIgnoresMissing", func(t *testing.T) {
		store := newStore(t)
		require.NoError(t, store.Put(t.Context(), "a", strings.NewReader("")))
		require.NoError(t, store.Put(t.Context(), "b", strings.NewReader("")))
		require.NoError(t, store.Delete(t.Context(), []string{"a", "missing"}))
		_, err := store.Get(t.Context(), "a")
		assert.ErrorIs(t, err, blob.ErrNotFound)
		assert.Equal(t, "", string(get(t, store, "b")))
	})
	t.Run("DeleteBatchesManyKeys", func(t *testing.T) {
		store := newStore(t)
		const count = 1100
		keys := make([]string, 0, count)
		for i := range count {
			key := fmt.Sprintf("d/%05d", i)
			keys = append(keys, key)
			require.NoError(t, store.Put(t.Context(), key, strings.NewReader("")))
		}
		require.NoError(t, store.Delete(t.Context(), keys))
		assert.Empty(t, list(t, store, "d/"))
	})
	t.Run("CanceledContextFails", func(t *testing.T) {
		store := newStore(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		assert.Error(t, store.Put(ctx, "k", strings.NewReader("v")))
		_, err := store.Get(ctx, "k")
		assert.Error(t, err)
	})
}

func get(t *testing.T, store blob.Store, key string) []byte {
	t.Helper()
	r, err := store.Get(t.Context(), key)
	require.NoError(t, err)
	defer func() { assert.NoError(t, r.Close()) }()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return data
}

func list(t *testing.T, store blob.Store, prefix string) []blob.Object {
	t.Helper()
	var objects []blob.Object
	require.NoError(t, store.List(t.Context(), prefix, func(object blob.Object) error {
		objects = append(objects, object)
		return nil
	}))
	return objects
}
