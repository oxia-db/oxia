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

package backup

import (
	"hash"
	"hash/crc32"
	"io"

	"github.com/klauspost/compress/zstd"
)

// crc32cTable is the table of the CRC32C checksums of the files.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func newCRC32C() hash.Hash32 {
	return crc32.New(crc32cTable)
}

// compress returns a reader of the zstd compression of r. The compression
// runs on another goroutine while the reader is read; a read error of r
// ends the reader with it.
func compress(r io.Reader) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		// Closing the pipe writer never fails
		encoder, err := zstd.NewWriter(pw)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if _, err := io.Copy(encoder, r); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = pw.CloseWithError(encoder.Close())
	}()
	return pr
}

// decompress returns a reader of the zstd decompression of r.
func decompress(r io.Reader) (io.ReadCloser, error) {
	decoder, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	return decoder.IOReadCloser(), nil
}

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
