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

package manifest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/oxia-db/oxia/oxiad/common/errs"

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

func TestNewManifestCorruptedFile(t *testing.T) {
	baseDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, filename), []byte("{not-json"), 0600))

	_, err := NewManifest(baseDir)
	assert.Error(t, err)
}

func TestNewManifestUnreadableFile(t *testing.T) {
	baseDir := t.TempDir()
	// A directory in place of the file can't be read as a file
	require.NoError(t, os.Mkdir(filepath.Join(baseDir, filename), 0755))

	_, err := NewManifest(baseDir)
	assert.Error(t, err)
}

func TestNewManifestFailsToCreateFile(t *testing.T) {
	parentDir := t.TempDir()
	makeReadOnly(t, parentDir)

	_, err := NewManifest(filepath.Join(parentDir, "db"))
	assert.ErrorIs(t, err, os.ErrPermission)
}

func TestManifestSetInstanceID(t *testing.T) {
	baseDir := t.TempDir()
	m, err := NewManifest(baseDir)
	require.NoError(t, err)

	bound, err := m.SetInstanceID("instance-1")
	require.NoError(t, err)
	assert.True(t, bound)
	assert.Equal(t, "instance-1", m.GetInstanceID())

	// Setting the same id again is a no-op
	bound, err = m.SetInstanceID("instance-1")
	require.NoError(t, err)
	assert.False(t, bound)

	// The id survives a restart
	reopened, err := NewManifest(baseDir)
	require.NoError(t, err)
	assert.Equal(t, "instance-1", reopened.GetInstanceID())
}

func TestManifestSetInstanceIDEmpty(t *testing.T) {
	m, err := NewManifest(t.TempDir())
	require.NoError(t, err)

	_, err = m.SetInstanceID("")
	assert.Error(t, err)
	assert.Empty(t, m.GetInstanceID())
}

func TestManifestSetInstanceIDMismatch(t *testing.T) {
	m, err := NewManifest(t.TempDir())
	require.NoError(t, err)
	_, err = m.SetInstanceID("instance-1")
	require.NoError(t, err)

	_, err = m.SetInstanceID("instance-2")
	assert.ErrorIs(t, err, errs.ErrInstanceIDMismatch)
	assert.Equal(t, "instance-1", m.GetInstanceID())
}

func TestManifestSetInstanceIDWriteError(t *testing.T) {
	baseDir := t.TempDir()
	m, err := NewManifest(baseDir)
	require.NoError(t, err)

	makeReadOnly(t, baseDir)

	_, err = m.SetInstanceID("instance-1")
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.Empty(t, m.GetInstanceID())
}

func TestReadInstanceID(t *testing.T) {
	baseDir := t.TempDir()
	m, err := NewManifest(baseDir)
	require.NoError(t, err)
	_, err = m.SetInstanceID("instance-1")
	require.NoError(t, err)

	instanceID, err := ReadInstanceID(baseDir)
	require.NoError(t, err)
	assert.Equal(t, "instance-1", instanceID)
}

func TestReadInstanceIDMissingManifest(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "db")

	instanceID, err := ReadInstanceID(baseDir)
	require.NoError(t, err)
	assert.Empty(t, instanceID)

	_, err = os.Stat(baseDir)
	assert.True(t, os.IsNotExist(err))
}

func TestReadInstanceIDCorruptedFile(t *testing.T) {
	baseDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, filename), []byte("{not-json"), 0600))

	_, err := ReadInstanceID(baseDir)
	assert.Error(t, err)
}

func makeReadOnly(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })
}
