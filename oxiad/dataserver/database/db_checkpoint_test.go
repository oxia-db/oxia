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

package database

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	commontime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

func TestDBCheckpointHoldsTheEntriesUpToItsCommitOffset(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	db, err := NewDB("ns", 1, factory, proto.KeySortingType_NATURAL, 0, commontime.SystemClock)
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()

	require.NoError(t, db.UpdateTerm(3, TermOptions{}))
	for offset := range int64(5) {
		_, err := db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "k", Value: []byte("v")}}},
			offset, 0, NoOpCallback)
		require.NoError(t, err)
	}

	checkpoint, err := db.Checkpoint()
	require.NoError(t, err)
	assert.EqualValues(t, 4, checkpoint.CommitOffset)
	assert.EqualValues(t, 3, checkpoint.Term)
	entries, err := os.ReadDir(checkpoint.BasePath())
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	hasSST := false
	for _, entry := range entries {
		names = append(names, entry.Name())
		if len(entry.Name()) > 4 && entry.Name()[len(entry.Name())-4:] == ".sst" {
			hasSST = true
		}
	}
	assert.True(t, hasSST, "the flush before the checkpoint wrote the entries to an SST: %v", names)
	assert.Contains(t, names, "oxia-key-encoding-format")

	require.NoError(t, checkpoint.Close())
	_, err = os.Stat(checkpoint.BasePath())
	assert.True(t, os.IsNotExist(err), "closing the checkpoint deletes it")
}

func TestDBCheckpointOfAnEmptyDatabase(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	db, err := NewDB("ns", 1, factory, proto.KeySortingType_NATURAL, 0, commontime.SystemClock)
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()

	checkpoint, err := db.Checkpoint()
	require.NoError(t, err)
	defer func() { assert.NoError(t, checkpoint.Close()) }()
	assert.EqualValues(t, -1, checkpoint.CommitOffset)
	assert.EqualValues(t, -1, checkpoint.Term)
}
