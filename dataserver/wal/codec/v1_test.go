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

package codec

import (
	"encoding/binary"
	"os"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestV1_GetHeaderSize(t *testing.T) {
	assert.EqualValues(t, v1.GetHeaderSize(), 4)
}

func TestV1_Codec(t *testing.T) {
	buf := make([]byte, 100)
	payload := []byte{1}
	recordSize, _ := v1.WriteRecord(buf, 0, 0, payload)
	assert.EqualValues(t, recordSize, 5)
	getRecordSize, err := v1.GetRecordSize(buf, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, getRecordSize, recordSize)

	getPayload, err := v1.ReadRecordWithValidation(buf, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload, getPayload)
}

func TestV1_WriteReadIndex(t *testing.T) {
	dir := os.TempDir()
	fileName := "0"
	elementsNum := 5
	indexBuf := make([]byte, uint32(elementsNum*4)+v1.GetIndexHeaderSize())
	for i := 0; i < elementsNum; i++ {
		binary.BigEndian.PutUint32(indexBuf[i*4:], uint32(i))
	}
	p := path.Join(dir, fileName+v1.GetIdxExtension())
	err := v1.WriteIndex(p, indexBuf)
	assert.NoError(t, err)
	index, err := v1.ReadIndex(p)
	assert.NoError(t, err)
	for i := 0; i < elementsNum; i++ {
		idx := ReadInt(index, uint32(i*4))
		assert.EqualValues(t, idx, i)
	}
}

func TestV1_RecoverIndex(t *testing.T) {
	elementsNum := 5

	buf := make([]byte, 100)
	var payloads [][]byte
	for i := 0; i < elementsNum; i++ {
		payload, err := uuid.New().MarshalBinary()
		assert.NoError(t, err)
		payloads = append(payloads, payload)
	}

	fOffset := uint32(0)
	for i := 0; i < elementsNum; i++ {
		recordSize, _ := v1.WriteRecord(buf, fOffset, 0, payloads[i])
		fOffset += recordSize
	}

	index, _, newFileOffset, lastEntryOffset, err := v1.RecoverIndex(buf, 0, 0, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, lastEntryOffset, 4)
	assert.EqualValues(t, fOffset, newFileOffset)
	for i := 0; i < elementsNum; i++ {
		fOffset := ReadInt(index, uint32(i*4))
		payload, err := v1.ReadRecordWithValidation(buf, fOffset)
		assert.NoError(t, err)
		assert.EqualValues(t, payloads[i], payload)
	}
}

func TestV1_RecoverIndex0(t *testing.T) {
	elementsNum := 5

	buf := make([]byte, 100)
	var payloads [][]byte
	for i := 0; i < elementsNum; i++ {
		payload, err := uuid.New().MarshalBinary()
		assert.NoError(t, err)
		payloads = append(payloads, payload)
	}

	fOffset := uint32(0)
	for i := 0; i < elementsNum; i++ {
		recordSize, _ := v1.WriteRecord(buf, fOffset, 0, payloads[i])
		fOffset += recordSize
	}

	file, err := os.CreateTemp("", "TestV1_RecoverIndex0")
	assert.NoError(t, err)
	defer os.Remove(file.Name())
	_, err = file.Write(buf)
	assert.NoError(t, err)

	stats, err := file.Stat()
	assert.NoError(t, err)
	index, _, newFileOffset, lastEntryOffset, err := v1.RecoverIndex0(NewFileReader(file, stats), 0, 0, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, lastEntryOffset, 4)
	assert.EqualValues(t, fOffset, newFileOffset)
	for i := 0; i < elementsNum; i++ {
		fOffset := ReadInt(index, uint32(i*4))
		payload, err := v1.ReadRecordWithValidation(buf, fOffset)
		assert.NoError(t, err)
		assert.EqualValues(t, payloads[i], payload)
	}
}
