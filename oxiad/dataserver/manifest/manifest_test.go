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

package manifest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewManifestCreatesDirectoryAndFile(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "db")

	m, err := NewManifest(baseDir)
	require.NoError(t, err)
	require.NotNil(t, m)

	_, err = os.Stat(baseDir)
	assert.NoError(t, err)

	info, err := os.Stat(filepath.Join(baseDir, filename))
	require.NoError(t, err)
	assert.False(t, info.IsDir())

	assert.Empty(t, m.GetInstanceID())
}
