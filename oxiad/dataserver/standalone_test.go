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

package dataserver

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStandaloneRejectsSameWalAndDataDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	config := NewTestConfig(t.TempDir())
	config.DataServerOptions.Storage.WAL.Dir = dir
	config.DataServerOptions.Storage.Database.Dir = dir

	standaloneServer, err := NewStandalone(config)
	assert.ErrorContains(t, err, "are the same directory")
	assert.Nil(t, standaloneServer)

	// Refused before writing anything
	_, err = os.Stat(dir)
	assert.ErrorIs(t, err, os.ErrNotExist)
}
