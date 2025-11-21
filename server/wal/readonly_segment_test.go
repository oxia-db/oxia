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

package wal

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/server/wal/codec"
)

func TestReadOnlySegment(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	for i := int64(0); i < 5; i++ {
		assert.NoError(t, rw.Append(i, []byte(fmt.Sprintf("entry-%d", i))))
	}
	assert.NoError(t, rw.Flush())
	for i := int64(5); i < 10; i++ {
		assert.NoError(t, rw.Append(i, []byte(fmt.Sprintf("entry-%d", i))))
	}
	assert.NoError(t, rw.Flush())

	tnxBuf := rw.(*readWriteSegment).txnBuf
	tnxFile := rw.(*readWriteSegment).txnFile
	_, err = tnxFile.Seek(0, io.SeekStart)
	assert.NoError(t, err)

	bts, err := io.ReadAll(tnxFile)
	assert.NoError(t, err)
	bts = bts[:len(bts)-1] // see: readWriteSegment.initFileWithZeroes

	bufStr := string(tnxBuf)
	fileStr := string(bts)
	assert.True(t, strings.EqualFold(bufStr, fileStr))

	assert.NoError(t, rw.Close())

	ro, err := newReadOnlySegment(path, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, ro.BaseOffset())
	assert.EqualValues(t, 9, ro.LastOffset())

	for i := int64(0); i < 10; i++ {
		data, err := ro.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(data))
	}

	data, err := ro.Read(100)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	data, err = ro.Read(-1)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	assert.NoError(t, ro.Close())
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
		data, err := ro.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(data))
	}

	data, err := ro.Read(100)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	data, err = ro.Read(-1)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	assert.NoError(t, ro.Close())
}
