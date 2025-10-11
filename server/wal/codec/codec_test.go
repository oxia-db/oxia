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
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodec_GetOrCreate(t *testing.T) {
	baseDir := os.TempDir()
	nonExistFileName := "0"
	v1FileName := "1"
	v2FileName := "2"
	_, err := os.Create(path.Join(baseDir, v1FileName+v1.GetTxnExtension()))
	assert.NoError(t, err)
	_, err = os.Create(path.Join(baseDir, v2FileName+v2.GetTxnExtension()))
	assert.NoError(t, err)

	codec, exist, err := GetOrCreate(path.Join(baseDir, nonExistFileName))
	assert.NoError(t, err)
	assert.EqualValues(t, v2, codec)
	assert.EqualValues(t, false, exist)

	codec, exist, err = GetOrCreate(path.Join(baseDir, v1FileName))
	assert.NoError(t, err)
	assert.EqualValues(t, v1, codec)
	assert.EqualValues(t, true, exist)

	codec, exist, err = GetOrCreate(path.Join(baseDir, v2FileName))
	assert.NoError(t, err)
	assert.EqualValues(t, v2, codec)
	assert.EqualValues(t, true, exist)
}
