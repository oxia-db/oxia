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
	"hash/crc32"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashingReaderChecksumsTheLastPassFromTheStart(t *testing.T) {
	content := "the content of a file"
	h := newHashingReader(strings.NewReader(content))
	half := make([]byte, 5)
	_, err := io.ReadFull(h, half)
	require.NoError(t, err)
	// A store that sizes the body seeks around without reading
	end, err := h.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	assert.EqualValues(t, len(content), end)
	// A send retried from the start replaces the first, partial, pass
	_, err = h.Seek(0, io.SeekStart)
	require.NoError(t, err)
	data, err := io.ReadAll(h)
	require.NoError(t, err)
	assert.Equal(t, content, string(data))
	assert.EqualValues(t, len(content), h.size)
	assert.Equal(t, crc32.Checksum([]byte(content), crc32cTable), h.crc.Sum32())
}
