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
