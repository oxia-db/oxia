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
)

// crc32cTable is the table of the CRC32C checksums of the files.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func newCRC32C() hash.Hash32 {
	return crc32.New(crc32cTable)
}

// hashingReader checksums and counts what is read through it, for the file
// a store uploads: a store may rewind the file to send it again, and the
// checksum and the size are then those of the last pass from the start.
type hashingReader struct {
	io.ReadSeeker
	crc  hash.Hash32
	size int64
}

func newHashingReader(r io.ReadSeeker) *hashingReader {
	return &hashingReader{ReadSeeker: r, crc: newCRC32C()}
}

func (h *hashingReader) Read(p []byte) (int, error) {
	n, err := h.ReadSeeker.Read(p)
	// Writing to a hash never fails
	_, _ = h.crc.Write(p[:n])
	h.size += int64(n)
	return n, err
}

func (h *hashingReader) Seek(offset int64, whence int) (int64, error) {
	if offset == 0 && whence == io.SeekStart {
		h.crc.Reset()
		h.size = 0
	}
	return h.ReadSeeker.Seek(offset, whence)
}
