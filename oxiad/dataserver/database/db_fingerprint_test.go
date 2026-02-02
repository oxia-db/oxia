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

package database

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

// Helper function to access private methods for testing.
func getDbFingerprint(database DB) (current uint64, stored uint64, err error) {
	// Access the internal db struct through unsafe type assertion for testing
	if dbImpl, ok := database.(*db); ok {
		current = dbImpl.getCurrentDbFingerprint()
		stored, err := dbImpl.readDbFingerprint()
		return current, stored, err
	}
	return 0, 0, errors.New("unexpected db type")
}

func TestDB_FingerprintConsistency(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)

	database, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 0, time.SystemClock)
	require.NoError(t, err)
	defer database.Close()

	// Test fingerprint consistency across multiple writes
	writeReq1 := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "key1", Value: []byte("value1")},
			{Key: "key2", Value: []byte("value2")},
		},
	}

	writeReq2 := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "key3", Value: []byte("value3")},
		},
		Deletes: []*proto.DeleteRequest{
			{Key: "key1"},
		},
	}

	// Get initial fingerprint (should be 0)
	initialCurrent, initialStored, err := getDbFingerprint(database)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), initialCurrent)
	assert.Equal(t, uint64(0), initialStored)

	// Perform first write
	resp1, err := database.ProcessWrite(writeReq1, 1, 1000, NoOpCallback)
	require.NoError(t, err)
	require.NotNil(t, resp1)

	// Get fingerprint after first write
	fingerprint1, stored1, err := getDbFingerprint(database)
	require.NoError(t, err)
	assert.NotZero(t, fingerprint1)
	assert.Equal(t, fingerprint1, stored1)

	// Perform second write
	resp2, err := database.ProcessWrite(writeReq2, 2, 2000, NoOpCallback)
	require.NoError(t, err)
	require.NotNil(t, resp2)

	// Get fingerprint after second write
	fingerprint2, stored2, err := getDbFingerprint(database)
	require.NoError(t, err)
	assert.NotZero(t, fingerprint2)
	assert.NotEqual(t, fingerprint1, fingerprint2)
	assert.Equal(t, fingerprint2, stored2)
}

func TestDB_FingerprintDeterministic(t *testing.T) {
	factory1, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)

	factory2, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)

	db1, err := NewDB(constant.DefaultNamespace, 1, factory1, proto.KeySortingType_NATURAL, 0, time.SystemClock)
	require.NoError(t, err)
	defer db1.Close()

	db2, err := NewDB(constant.DefaultNamespace, 1, factory2, proto.KeySortingType_NATURAL, 0, time.SystemClock)
	require.NoError(t, err)
	defer db2.Close()

	// Same write operations on both databases
	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "key1", Value: []byte("value1")},
			{Key: "key2", Value: []byte("value2")},
		},
	}

	// Perform writes with same parameters
	_, err = db1.ProcessWrite(writeReq, 1, 1000, NoOpCallback)
	require.NoError(t, err)

	_, err = db2.ProcessWrite(writeReq, 1, 1000, NoOpCallback)
	require.NoError(t, err)

	// Fingerprints should be identical for same operations
	fingerprint1, _, err := getDbFingerprint(db1)
	require.NoError(t, err)
	fingerprint2, _, err := getDbFingerprint(db2)
	require.NoError(t, err)
	assert.Equal(t, fingerprint1, fingerprint2)
}

func TestDB_FingerprintRestart(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)

	// Create database and perform some writes
	db1, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 0, time.SystemClock)
	require.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "key1", Value: []byte("value1")},
			{Key: "key2", Value: []byte("value2")},
		},
	}

	_, err = db1.ProcessWrite(writeReq, 1, 1000, NoOpCallback)
	require.NoError(t, err)

	// Get fingerprint before restart
	fingerprintBeforeRestart, _, err := getDbFingerprint(db1)
	require.NoError(t, err)
	require.NotZero(t, fingerprintBeforeRestart)

	// Close database (simulating restart)
	err = db1.Close()
	require.NoError(t, err)

	// Reopen database
	db2, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 0, time.SystemClock)
	require.NoError(t, err)
	defer db2.Close()

	// Fingerprint should be preserved after restart
	fingerprintAfterRestart, _, err := getDbFingerprint(db2)
	require.NoError(t, err)
	assert.Equal(t, fingerprintBeforeRestart, fingerprintAfterRestart)
}
