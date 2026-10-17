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

package blob_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/backup/blob"
	"github.com/oxia-db/oxia/oxiad/backup/blob/blobtest"
)

func TestMemoryConformance(t *testing.T) {
	blobtest.RunConformance(t, func(*testing.T) blob.Store {
		return blob.NewMemory()
	})
}

func TestMemoryStampsObjectsWithItsClock(t *testing.T) {
	store := blob.NewMemory()
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	store.SetClock(func() time.Time { return now })
	require.NoError(t, store.Put(t.Context(), "k", strings.NewReader("v")))
	var objects []blob.Object
	require.NoError(t, store.List(t.Context(), "", func(object blob.Object) error {
		objects = append(objects, object)
		return nil
	}))
	require.Len(t, objects, 1)
	assert.Equal(t, now, objects[0].LastModified)
}

func TestMemoryFailPutInterruptsWrites(t *testing.T) {
	store := blob.NewMemory()
	store.FailPut(func(key string) error {
		if key == "bad" {
			return assert.AnError
		}
		return nil
	})
	require.NoError(t, store.Put(t.Context(), "good", strings.NewReader("v")))
	assert.ErrorIs(t, store.Put(t.Context(), "bad", strings.NewReader("v")), assert.AnError)
	assert.Equal(t, []string{"good"}, store.Keys())
}

func TestOpenResolvesSharedMemoryByName(t *testing.T) {
	store, err := blob.Open(t.Context(), "mem://"+t.Name())
	require.NoError(t, err)
	require.NoError(t, store.Put(t.Context(), "k", strings.NewReader("v")))
	again, err := blob.Open(t.Context(), "mem://"+t.Name())
	require.NoError(t, err)
	assert.Equal(t, []string{"k"}, again.(*blob.Memory).Keys())
	other, err := blob.Open(t.Context(), "mem://"+t.Name()+"-other")
	require.NoError(t, err)
	assert.Empty(t, other.(*blob.Memory).Keys())
}

func TestOpenRejectsUnknownScheme(t *testing.T) {
	_, err := blob.Open(t.Context(), "ftp://host/path")
	assert.ErrorContains(t, err, `unsupported destination scheme "ftp"`)
}
