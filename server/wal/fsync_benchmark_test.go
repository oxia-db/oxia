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
	"os"
	"path/filepath"
	"testing"

	"github.com/edsrzf/mmap-go"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
)

const (
	testFile  = "test-file.bin"
	writeSize = 4096
)

func BenchmarkFSync_stdlib(b *testing.B) {
	f, err := os.Create(filepath.Join(b.TempDir(), testFile))
	assert.NoError(b, err)
	data := make([]byte, writeSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := f.Write(data)
		assert.NoError(b, err)

		err = f.Sync()
		assert.NoError(b, err)
	}

	b.StopTimer()
}

func BenchmarkFSync_x_sys(b *testing.B) {
	f, err := os.Create(filepath.Join(b.TempDir(), testFile))
	assert.NoError(b, err)
	data := make([]byte, writeSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := f.Write(data)
		assert.NoError(b, err)

		err = unix.Fsync(int(f.Fd()))
		assert.NoError(b, err)
	}

	b.StopTimer()
}

func BenchmarkMSync(b *testing.B) {
	f, err := os.Create(filepath.Join(b.TempDir(), testFile))
	assert.NoError(b, err)

	data := make([]byte, writeSize)

	_, err = f.Seek(int64(writeSize*b.N), 0)
	assert.NoError(b, err)
	_, err = f.Write([]byte{0x00})
	assert.NoError(b, err)
	err = f.Sync()
	assert.NoError(b, err)

	assert.NoError(b, f.Sync())

	mappedFile, err := mmap.MapRegion(f, writeSize*b.N, mmap.RDWR, 0, 0)
	assert.NoError(b, err)

	b.ResetTimer()

	offset := 0
	for i := 0; i < b.N; i++ {
		copy(mappedFile[offset:], data)

		err = mappedFile.Flush()
		assert.NoError(b, err)

		offset += writeSize
	}

	b.StopTimer()

	assert.NoError(b, mappedFile.Unmap())
	assert.NoError(b, f.Close())
}
