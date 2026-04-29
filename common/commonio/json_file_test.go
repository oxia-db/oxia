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

package commonio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadJSONFromFile(t *testing.T) {
	t.Run("read existing json", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		require.NoError(t, os.WriteFile(path, []byte(`{"hello":"world"}`), 0o600))

		value := map[string]string{}
		hasContent, err := ReadJSONFromFile(path, &value)
		require.NoError(t, err)
		assert.True(t, hasContent)
		assert.Equal(t, map[string]string{"hello": "world"}, value)
	})

	t.Run("empty file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "state.json")
		require.NoError(t, os.WriteFile(path, nil, 0o600))

		value := map[string]string{"existing": "value"}
		hasContent, err := ReadJSONFromFile(path, &value)
		require.NoError(t, err)
		assert.False(t, hasContent)
		assert.Equal(t, map[string]string{"existing": "value"}, value)
	})

	t.Run("missing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.json")

		value := map[string]string{}
		hasContent, err := ReadJSONFromFile(path, &value)
		require.Error(t, err)
		assert.True(t, os.IsNotExist(err))
		assert.False(t, hasContent)
	})
}

func TestWriteJSONAtomicallyCreatesParentDirectoryAndFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "state.json")

	err := WriteJSONToFile(path, map[string]string{
		"hello": "world",
	})
	require.NoError(t, err)

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.JSONEq(t, `{"hello":"world"}`, string(content))
}
