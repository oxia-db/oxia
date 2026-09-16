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

package wal

import (
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal/codec"
)

func TestReadOnlySegment(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		assert.NoError(t, rw.Append(i, []byte(fmt.Sprintf("entry-%d", i))))
	}
	assert.NoError(t, rw.Close())

	ro, err := newReadOnlySegment(path, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, ro.BaseOffset())
	assert.EqualValues(t, 9, ro.LastOffset())

	for i := int64(0); i < 10; i++ {
		data, _, _, err := ro.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(data))
	}

	data, _, _, err := ro.Read(100)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	data, _, _, err = ro.Read(-1)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	assert.NoError(t, ro.Close())
}

// An empty index file must trigger an index rebuild, not be trusted (it used
// to panic with a slice out of bounds).
func TestRO_auto_recover_empty_index(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		assert.NoError(t, rw.Append(i, []byte(fmt.Sprintf("entry-%d", i))))
	}
	rwSegment := rw.(*readWriteSegment)
	assert.NoError(t, rw.Close())

	// Replace the index with a valid empty one, as written by the close of a
	// previous, then-empty incarnation of the segment
	assert.NoError(t, os.Remove(rwSegment.c.idxPath))
	assert.NoError(t, rwSegment.c.codec.WriteIndex(rwSegment.c.idxPath, nil))

	ro, err := newReadOnlySegment(path, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, ro.BaseOffset())
	assert.EqualValues(t, 9, ro.LastOffset())

	for i := int64(0); i < 10; i++ {
		data, _, _, err := ro.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(data))
	}

	assert.NoError(t, ro.Close())
}

// A segment whose txn file holds no entries cannot be opened for reads: it
// must fail with ErrEmptySegment (it used to panic with a slice out of
// bounds), regardless of whether an index file is present.
func TestRO_EmptySegment(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	rwSegment := rw.(*readWriteSegment)
	assert.NoError(t, rw.Close())

	_, err = os.Stat(rwSegment.c.idxPath)
	assert.ErrorIs(t, err, os.ErrNotExist)

	ro, err := newReadOnlySegment(path, 0)
	assert.Nil(t, ro)
	assert.ErrorIs(t, err, ErrEmptySegment)
}

func TestRO_auto_recover_broken_index(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		assert.NoError(t, rw.Append(i, []byte(fmt.Sprintf("entry-%d", i))))
	}
	rwSegment := rw.(*readWriteSegment)
	assert.NoError(t, rw.Close())

	idxPath := rwSegment.c.idxPath
	// inject wrong data
	file, err := os.OpenFile(idxPath, os.O_RDWR, 0644)
	assert.NoError(t, err)
	defer file.Close()
	faultData, err := uuid.New().MarshalBinary()
	assert.NoError(t, err)
	_, err = file.WriteAt(faultData, 0)
	assert.NoError(t, err)

	ro, err := newReadOnlySegment(path, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, ro.BaseOffset())
	assert.EqualValues(t, 9, ro.LastOffset())

	for i := int64(0); i < 10; i++ {
		data, _, _, err := ro.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(data))
	}

	data, _, _, err := ro.Read(100)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	data, _, _, err = ro.Read(-1)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	assert.NoError(t, ro.Close())
}
