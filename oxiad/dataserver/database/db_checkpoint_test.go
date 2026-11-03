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
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

func TestReadCheckpointReturnsTheCommitOffsetAndTermOfTheFiles(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	db, err := NewDB("ns", 1, factory, proto.KeySortingType_NATURAL, 0, commontime.SystemClock)
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()

	require.NoError(t, db.UpdateTerm(3, TermOptions{}))
	for offset := range int64(5) {
		_, err := db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "k", Value: []byte("v")}}},
			offset, 0, 0, NoOpCallback)
		require.NoError(t, err)
	}

	snapshot, err := db.Snapshot()
	require.NoError(t, err)
	// The files are what the checkpoint is: a later write doesn't change them
	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "k", Value: []byte("w")}}},
		5, 0, 0, NoOpCallback)
	require.NoError(t, err)

	commitOffset, term, err := ReadCheckpoint(snapshot.BasePath())
	require.NoError(t, err)
	assert.EqualValues(t, 4, commitOffset)
	assert.EqualValues(t, 3, term)

	require.NoError(t, snapshot.Close())
	_, err = os.Stat(snapshot.BasePath())
	assert.True(t, os.IsNotExist(err), "closing the snapshot deletes it")
}

func TestReadCheckpointFailsWithoutDatabase(t *testing.T) {
	commitOffset, term, err := ReadCheckpoint(t.TempDir())
	assert.ErrorContains(t, err, "failed to open the checkpoint")
	assert.EqualValues(t, wal.InvalidOffset, commitOffset)
	assert.EqualValues(t, wal.InvalidTerm, term)
}

func TestReadCheckpointFailsOnCorruptState(t *testing.T) {
	for key, message := range map[string]string{
		commitOffsetKey: "failed to read the commit offset of the checkpoint",
		termKey:         "failed to read the term of the checkpoint",
	} {
		t.Run(key, func(t *testing.T) {
			factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
			require.NoError(t, err)
			defer func() { assert.NoError(t, factory.Close()) }()
			kv, err := factory.NewKV("ns", 1, proto.KeySortingType_NATURAL)
			require.NoError(t, err)
			defer func() { assert.NoError(t, kv.Close()) }()
			batch := kv.NewWriteBatch()
			require.NoError(t, batch.Put(key, []byte{0xff, 0xff}))
			require.NoError(t, batch.Commit())
			require.NoError(t, batch.Close())
			snapshot, err := kv.Snapshot()
			require.NoError(t, err)
			defer func() { assert.NoError(t, snapshot.Close()) }()

			commitOffset, term, err := ReadCheckpoint(snapshot.BasePath())
			assert.ErrorContains(t, err, message)
			assert.EqualValues(t, wal.InvalidOffset, commitOffset)
			assert.EqualValues(t, wal.InvalidTerm, term)
		})
	}
}
