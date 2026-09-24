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

package option

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageOptionsRejectsSameWalAndDataDir(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(dir, link))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "wal"), 0755))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "db"), 0755))

	for _, tt := range []struct {
		name    string
		walDir  string
		dataDir string
		same    bool
	}{
		{name: "same path", walDir: dir, dataDir: dir, same: true},
		{name: "same path spelled differently", walDir: dir + "/./", dataDir: dir, same: true},
		{name: "relative and absolute path", walDir: ".", dataDir: dir, same: true},
		{name: "symlink to the data dir", walDir: link, dataDir: dir, same: true},
		{name: "different dirs", walDir: filepath.Join(dir, "wal"), dataDir: filepath.Join(dir, "db")},
		{name: "wal dir inside the data dir", walDir: filepath.Join(dir, "wal"), dataDir: dir},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Chdir(dir)
			options := StorageOptions{}
			options.WithDefault()
			options.WAL.Dir = tt.walDir
			options.Database.Dir = tt.dataDir

			err := options.Validate()
			if tt.same {
				assert.ErrorContains(t, err, "are the same directory")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
