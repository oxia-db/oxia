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
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressThenDecompressRoundTrips(t *testing.T) {
	content := bytes.Repeat([]byte("the quick brown fox "), 10_000)
	compressed, err := io.ReadAll(compress(bytes.NewReader(content)))
	require.NoError(t, err)
	assert.Less(t, len(compressed), len(content)/10)
	decompressed, err := decompress(bytes.NewReader(compressed))
	require.NoError(t, err)
	defer decompressed.Close()
	data, err := io.ReadAll(decompressed)
	require.NoError(t, err)
	assert.Equal(t, content, data)
}

func TestCompressPropagatesAReadError(t *testing.T) {
	_, err := io.ReadAll(compress(io.MultiReader(strings.NewReader("abc"), &failingReader{})))
	assert.ErrorIs(t, err, assert.AnError)
}

func TestCountingReaderCountsTheBytesRead(t *testing.T) {
	r := &countingReader{r: strings.NewReader("hello")}
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(data))
	assert.EqualValues(t, 5, r.n)
}

type failingReader struct{}

func (*failingReader) Read([]byte) (int, error) {
	return 0, assert.AnError
}
