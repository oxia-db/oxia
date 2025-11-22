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

//revive:disable-next-line:var-naming
package codec

import (
	"errors"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveFileIfExists(t *testing.T) {
	t.Run("file not exists", func(t *testing.T) {
		path := path.Join(t.TempDir(), "file_not_exists")
		_, err := os.Stat(path)
		assert.True(t, errors.Is(err, os.ErrNotExist))

		err = RemoveFileIfExists(path)
		assert.NoError(t, err)
	})

	t.Run("file exists", func(t *testing.T) {
		path := path.Join(t.TempDir(), "file_exists")
		f, err := os.Create(path)
		assert.NoError(t, err)
		assert.NoError(t, f.Close())

		err = RemoveFileIfExists(path)
		assert.NoError(t, err)

		_, err = os.Stat(path)
		assert.True(t, errors.Is(err, os.ErrNotExist))
	})
}
