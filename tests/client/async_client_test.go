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

package client

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

func init() {
	logging.LogJSON = false
	logging.ConfigureLogger()
}

func newKey() string {
	return fmt.Sprintf("/my-key-%d", time.Now().Nanosecond())
}

func TestAsyncClientImpl(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	client, err := oxia.NewAsyncClient(standaloneServer.ServiceAddr(), oxia.WithBatchLinger(0))
	assert.NoError(t, err)

	putResultA := <-client.Put("/a", []byte{0}, oxia.ExpectedRecordNotExists())
	assert.EqualValues(t, 0, putResultA.Version.VersionId)
	assert.EqualValues(t, 0, putResultA.Version.ModificationsCount)

	getResult := <-client.Get("/a")
	assert.Equal(t, oxia.GetResult{
		Key:     "/a",
		Value:   []byte{0},
		Version: putResultA.Version,
	}, getResult)

	putResultC1 := <-client.Put("/c", []byte{0}, oxia.ExpectedRecordNotExists())
	assert.EqualValues(t, 1, putResultC1.Version.VersionId)
	assert.EqualValues(t, 0, putResultC1.Version.ModificationsCount)

	putResultC2 := <-client.Put("/c", []byte{1}, oxia.ExpectedVersionId(putResultC1.Version.VersionId))
	assert.EqualValues(t, 2, putResultC2.Version.VersionId)
	assert.EqualValues(t, 1, putResultC2.Version.ModificationsCount)

	listResult := <-client.List(context.Background(), "/y", "/z")
	assert.Len(t, listResult.Keys, 0)

	listResult = <-client.List(context.Background(), "/a", "/d")
	assert.Equal(t, oxia.ListResult{
		Keys: []string{"/a", "/c"},
	}, listResult)

	deleteErr := <-client.Delete("/a", oxia.ExpectedVersionId(putResultA.Version.VersionId))
	assert.NoError(t, deleteErr)

	getResult = <-client.Get("/a")
	assert.Equal(t, oxia.GetResult{
		Err: oxia.ErrKeyNotFound,
	}, getResult)

	deleteRangeResult := <-client.DeleteRange("/c", "/d")
	assert.NoError(t, deleteRangeResult)

	getResult = <-client.Get("/d")
	assert.Equal(t, oxia.GetResult{
		Err: oxia.ErrKeyNotFound,
	}, getResult)

	err = client.Close()
	assert.NoError(t, err)

	err = standaloneServer.Close()
	assert.NoError(t, err)
}

func TestSyncClientImpl_Notifications(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	client, err := oxia.NewSyncClient(standaloneServer.ServiceAddr(), oxia.WithBatchLinger(0))
	assert.NoError(t, err)

	notifications, err := client.GetNotifications()
	assert.NoError(t, err)

	ctx := context.Background()

	_, s1, _ := client.Put(ctx, "/a", []byte("0"))

	n := <-notifications.Ch()
	assert.Equal(t, oxia.KeyCreated, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.Equal(t, s1.VersionId, n.VersionId)

	_, s2, _ := client.Put(ctx, "/a", []byte("1"))

	n = <-notifications.Ch()
	assert.Equal(t, oxia.KeyModified, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.Equal(t, s2.VersionId, n.VersionId)

	_, s3, _ := client.Put(ctx, "/b", []byte("0"))
	assert.NoError(t, client.Delete(ctx, "/a"))

	n = <-notifications.Ch()
	assert.Equal(t, oxia.KeyCreated, n.Type)
	assert.Equal(t, "/b", n.Key)
	assert.Equal(t, s3.VersionId, n.VersionId)

	n = <-notifications.Ch()
	assert.Equal(t, oxia.KeyDeleted, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.EqualValues(t, -1, n.VersionId)

	// Create a 2nd notifications channel
	// This will only receive new updates
	notifications2, err := client.GetNotifications()
	assert.NoError(t, err)

	select {
	case <-notifications2.Ch():
		assert.Fail(t, "shouldn't have received any notifications")
	case <-time.After(100 * time.Millisecond):
		// Ok, we expect it to time out
	}

	_, s4, _ := client.Put(ctx, "/x", []byte("1"))

	n = <-notifications.Ch()
	assert.Equal(t, oxia.KeyCreated, n.Type)
	assert.Equal(t, "/x", n.Key)
	assert.Equal(t, s4.VersionId, n.VersionId)

	n = <-notifications2.Ch()
	assert.Equal(t, oxia.KeyCreated, n.Type)
	assert.Equal(t, "/x", n.Key)
	assert.Equal(t, s4.VersionId, n.VersionId)

	assert.NoError(t, client.Close())

	// Channels should be closed after the client is closed
	select {
	case <-notifications.Ch():
		// Ok
	default:
		assert.Fail(t, "should have been closed")
	}

	select {
	case <-notifications2.Ch():
		// Ok

	default:
		assert.Fail(t, "should have been closed")
	}

	assert.NoError(t, standaloneServer.Close())
}

func TestAsyncClientImpl_NotificationsClose(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	client, err := oxia.NewSyncClient(standaloneServer.ServiceAddr(), oxia.WithBatchLinger(0))
	assert.NoError(t, err)

	notifications, err := client.GetNotifications()
	assert.NoError(t, err)

	assert.NoError(t, notifications.Close())

	select {
	case n := <-notifications.Ch():
		assert.Nil(t, n)

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Shouldn't have timed out")
	}

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestAsyncClientImpl_Sessions(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	client, err := oxia.NewAsyncClient(standaloneServer.ServiceAddr(), oxia.WithBatchLinger(0), oxia.WithSessionTimeout(5*time.Second))
	assert.NoError(t, err)

	putCh := client.Put("/x", []byte("x"), oxia.Ephemeral())
	versionId := atomic.Int64{}

	select {
	case res := <-putCh:
		assert.NotNil(t, res)
		assert.NoError(t, res.Err)
		assert.EqualValues(t, 0, res.Version.ModificationsCount)
		versionId.Store(res.Version.VersionId)

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Shouldn't have timed out")
	}
	getCh := client.Get("/x")
	select {
	case res := <-getCh:
		assert.NotNil(t, res)
		assert.NoError(t, res.Err)
		assert.EqualValues(t, 0, res.Version.ModificationsCount)
		assert.Equal(t, versionId.Load(), res.Version.VersionId)

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Shouldn't have timed out")
	}
	assert.NoError(t, client.Close())
	slog.Debug("First client closed")

	client, err = oxia.NewAsyncClient(standaloneServer.ServiceAddr(), oxia.WithBatchLinger(0))
	assert.NoError(t, err)
	assert.Eventually(t, func() bool {
		getCh = client.Get("/x")
		select {
		case res := <-getCh:
			assert.NotNil(t, res)
			slog.Debug(
				"Get resulted in",
				slog.Any("res", res),
			)
			return errors.Is(res.Err, oxia.ErrKeyNotFound)

		case <-time.After(1 * time.Second):
			assert.Fail(t, "Shouldn't have timed out")
			return false
		}
	}, 8*time.Second, 500*time.Millisecond)

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestAsyncClientImpl_OverrideEphemeral(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client, err := oxia.NewSyncClient(standaloneServer.ServiceAddr(),
		oxia.WithSessionTimeout(5*time.Second),
	)
	assert.NoError(t, err)

	k := newKey()
	_, version, err := client.Put(context.Background(), k, []byte("v1"), oxia.Ephemeral())
	assert.NoError(t, err)

	assert.True(t, version.Ephemeral)

	// Override with non-ephemeral value
	_, version, err = client.Put(context.Background(), k, []byte("v2"))
	assert.NoError(t, err)

	assert.False(t, version.Ephemeral)
	assert.Equal(t, "", version.ClientIdentity)

	assert.NoError(t, client.Close())

	// Reopen
	client, err = oxia.NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	var res []byte
	_, res, version, err = client.Get(context.Background(), k)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, version.ModificationsCount)
	assert.Equal(t, "v2", string(res))
	assert.False(t, version.Ephemeral)
	assert.Equal(t, "", version.ClientIdentity)
	assert.NoError(t, client.Close())
}

func TestAsyncClientImpl_ClientIdentity(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client1, err := oxia.NewSyncClient(standaloneServer.ServiceAddr(),
		oxia.WithIdentity("client-1"),
	)
	assert.NoError(t, err)

	k := newKey()
	_, version, err := client1.Put(context.Background(), k, []byte("v1"), oxia.Ephemeral())
	assert.NoError(t, err)

	assert.True(t, version.Ephemeral)
	assert.Equal(t, "client-1", version.ClientIdentity)

	client2, err := oxia.NewSyncClient(standaloneServer.ServiceAddr(),
		oxia.WithSessionTimeout(2*time.Second),
		oxia.WithIdentity("client-2"),
	)
	assert.NoError(t, err)

	var res []byte
	_, res, version, err = client2.Get(context.Background(), k)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version.ModificationsCount)
	assert.Equal(t, "v1", string(res))
	assert.True(t, version.Ephemeral)
	assert.Equal(t, "client-1", version.ClientIdentity)

	_, version, err = client2.Put(context.Background(), k, []byte("v2"), oxia.Ephemeral())
	assert.NoError(t, err)

	assert.True(t, version.Ephemeral)
	assert.Equal(t, "client-2", version.ClientIdentity)

	assert.NoError(t, client1.Close())
	assert.NoError(t, client2.Close())
}

func TestSyncClientImpl_SessionNotifications(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	client1, err := oxia.NewSyncClient(standaloneServer.ServiceAddr(), oxia.WithIdentity("client-1"))
	assert.NoError(t, err)

	client2, err := oxia.NewSyncClient(standaloneServer.ServiceAddr(), oxia.WithIdentity("client-1"))
	assert.NoError(t, err)

	notifications, err := client2.GetNotifications()
	assert.NoError(t, err)

	ctx := context.Background()

	_, s1, _ := client1.Put(ctx, "/a", []byte("0"), oxia.Ephemeral())

	n := <-notifications.Ch()
	assert.Equal(t, oxia.KeyCreated, n.Type)
	assert.Equal(t, "/a", n.Key)
	assert.Equal(t, s1.VersionId, n.VersionId)

	err = client1.Close()
	assert.NoError(t, err)

	select {
	case n = <-notifications.Ch():
		assert.Equal(t, oxia.KeyDeleted, n.Type)
		assert.Equal(t, "/a", n.Key)
	case <-time.After(3 * time.Second):
		assert.Fail(t, "read from channel timed out")
	}

	assert.NoError(t, client2.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestSyncClientImpl_FloorCeilingGet(t *testing.T) {
	config := dataserver.NewTestConfig(t.TempDir())
	// Test with multiple shards to ensure correctness across shards
	config.NumShards = 10
	standaloneServer, err := dataserver.NewStandalone(config)
	assert.NoError(t, err)

	client, err := oxia.NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	ctx := context.Background()
	_, _, _ = client.Put(ctx, "a", []byte("0"))
	// _, _, _ = client.Put(ctx, "b", []byte("1")) // Skipped intentionally
	_, _, _ = client.Put(ctx, "c", []byte("2"))
	_, _, _ = client.Put(ctx, "d", []byte("3"))
	_, _, _ = client.Put(ctx, "e", []byte("4"))
	// _, _, _ = client.Put(ctx, "f", []byte("5")) // Skipped intentionally
	_, _, _ = client.Put(ctx, "g", []byte("6"))

	key, value, _, err := client.Get(ctx, "a")
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(value))

	key, value, _, err = client.Get(ctx, "a", oxia.ComparisonEqual())
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(value))

	key, value, _, err = client.Get(ctx, "a", oxia.ComparisonFloor())
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(value))

	key, value, _, err = client.Get(ctx, "a", oxia.ComparisonCeiling())
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(value))

	_, _, _, err = client.Get(ctx, "a", oxia.ComparisonLower())
	assert.ErrorIs(t, oxia.ErrKeyNotFound, err)

	key, value, _, err = client.Get(ctx, "a", oxia.ComparisonHigher())
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(value))

	// ---------------------------------------------------------------

	_, _, _, err = client.Get(ctx, "b")
	assert.ErrorIs(t, oxia.ErrKeyNotFound, err)

	_, _, _, err = client.Get(ctx, "b", oxia.ComparisonEqual())
	assert.ErrorIs(t, oxia.ErrKeyNotFound, err)

	key, value, _, err = client.Get(ctx, "b", oxia.ComparisonFloor())
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(value))

	key, value, _, err = client.Get(ctx, "b", oxia.ComparisonCeiling())
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(value))

	key, value, _, err = client.Get(ctx, "b", oxia.ComparisonLower())
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(value))

	key, value, _, err = client.Get(ctx, "b", oxia.ComparisonHigher())
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(value))

	// ---------------------------------------------------------------

	key, value, _, err = client.Get(ctx, "c")
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(value))

	key, value, _, err = client.Get(ctx, "c", oxia.ComparisonEqual())
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(value))

	key, value, _, err = client.Get(ctx, "c", oxia.ComparisonFloor())
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(value))

	key, value, _, err = client.Get(ctx, "c", oxia.ComparisonCeiling())
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(value))

	key, value, _, err = client.Get(ctx, "c", oxia.ComparisonLower())
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(value))

	key, value, _, err = client.Get(ctx, "c", oxia.ComparisonHigher())
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(value))

	// ---------------------------------------------------------------

	key, value, _, err = client.Get(ctx, "d")
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(value))

	key, value, _, err = client.Get(ctx, "d", oxia.ComparisonEqual())
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(value))

	key, value, _, err = client.Get(ctx, "d", oxia.ComparisonFloor())
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(value))

	key, value, _, err = client.Get(ctx, "d", oxia.ComparisonCeiling())
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(value))

	key, value, _, err = client.Get(ctx, "d", oxia.ComparisonLower())
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(value))

	key, value, _, err = client.Get(ctx, "d", oxia.ComparisonHigher())
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(value))

	// ---------------------------------------------------------------

	key, value, _, err = client.Get(ctx, "e")
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(value))

	key, value, _, err = client.Get(ctx, "e", oxia.ComparisonEqual())
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(value))

	key, value, _, err = client.Get(ctx, "e", oxia.ComparisonFloor())
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(value))

	key, value, _, err = client.Get(ctx, "e", oxia.ComparisonCeiling())
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(value))

	key, value, _, err = client.Get(ctx, "e", oxia.ComparisonLower())
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(value))

	key, value, _, err = client.Get(ctx, "e", oxia.ComparisonHigher())
	assert.NoError(t, err)
	assert.Equal(t, "g", key)
	assert.Equal(t, "6", string(value))

	// ---------------------------------------------------------------

	_, _, _, err = client.Get(ctx, "f")
	assert.ErrorIs(t, oxia.ErrKeyNotFound, err)

	_, _, _, err = client.Get(ctx, "f", oxia.ComparisonEqual())
	assert.ErrorIs(t, oxia.ErrKeyNotFound, err)

	key, value, _, err = client.Get(ctx, "f", oxia.ComparisonFloor())
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(value))

	key, value, _, err = client.Get(ctx, "f", oxia.ComparisonCeiling())
	assert.NoError(t, err)
	assert.Equal(t, "g", key)
	assert.Equal(t, "6", string(value))

	key, value, _, err = client.Get(ctx, "f", oxia.ComparisonLower())
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(value))

	key, value, _, err = client.Get(ctx, "f", oxia.ComparisonHigher())
	assert.NoError(t, err)
	assert.Equal(t, "g", key)
	assert.Equal(t, "6", string(value))

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestSyncClientImpl_PartitionRouting(t *testing.T) {
	config := dataserver.NewTestConfig(t.TempDir())
	// Test with multiple shards to ensure correctness across shards
	config.NumShards = 10
	standaloneServer, err := dataserver.NewStandalone(config)
	assert.NoError(t, err)

	client, err := oxia.NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	ctx := context.Background()

	_, _, _ = client.Put(ctx, "a", []byte("0"), oxia.PartitionKey("x"))
	_, _, _, err = client.Get(ctx, "a")
	assert.ErrorIs(t, oxia.ErrKeyNotFound, err)

	key, value, _, err := client.Get(ctx, "a", oxia.PartitionKey("x"))
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(value))

	_, _, _ = client.Put(ctx, "a", []byte("0"), oxia.PartitionKey("x"))
	_, _, _ = client.Put(ctx, "b", []byte("1"), oxia.PartitionKey("x"))
	_, _, _ = client.Put(ctx, "c", []byte("2"), oxia.PartitionKey("x"))
	_, _, _ = client.Put(ctx, "d", []byte("3"), oxia.PartitionKey("x"))
	_, _, _ = client.Put(ctx, "e", []byte("4"), oxia.PartitionKey("x"))
	_, _, _ = client.Put(ctx, "f", []byte("5"), oxia.PartitionKey("x"))
	_, _, _ = client.Put(ctx, "g", []byte("6"), oxia.PartitionKey("x"))

	// Listing must yield the same results
	keys, err := client.List(ctx, "a", "d")
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, keys)

	keys, err = client.List(ctx, "a", "d", oxia.PartitionKey("x"))
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, keys)

	// Searching with wrong partition-key will return empty list
	keys, err = client.List(ctx, "a", "d", oxia.PartitionKey("wrong-partition-key"))
	assert.NoError(t, err)
	assert.Equal(t, []string{}, keys)

	// Delete with wrong partition key would fail
	err = client.Delete(ctx, "g", oxia.PartitionKey("wrong-partition-key"))
	assert.ErrorIs(t, err, oxia.ErrKeyNotFound)

	err = client.Delete(ctx, "g", oxia.PartitionKey("x"))
	assert.NoError(t, err)

	// Get tests
	key, value, _, err = client.Get(ctx, "a", oxia.ComparisonHigher())
	assert.NoError(t, err)
	assert.Equal(t, "b", key)
	assert.Equal(t, "1", string(value))

	key, value, _, err = client.Get(ctx, "a", oxia.ComparisonHigher(), oxia.PartitionKey("x"))
	assert.NoError(t, err)
	assert.Equal(t, "b", key)
	assert.Equal(t, "1", string(value))

	_, _, _, err = client.Get(ctx, "a", oxia.ComparisonHigher(), oxia.PartitionKey("wrong-partition-key"))
	assert.ErrorIs(t, err, oxia.ErrKeyNotFound)

	// Delete with wrong partition key would fail to delete all keys
	err = client.DeleteRange(ctx, "c", "e", oxia.PartitionKey("wrong-partition-key"))
	assert.NoError(t, err)

	keys, err = client.List(ctx, "c", "f")
	assert.NoError(t, err)
	assert.Equal(t, []string{"c", "d", "e"}, keys)

	err = client.DeleteRange(ctx, "c", "e", oxia.PartitionKey("x"))
	assert.NoError(t, err)

	keys, err = client.List(ctx, "c", "f")
	assert.NoError(t, err)
	assert.Equal(t, []string{"e"}, keys)

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestSyncClientImpl_SequentialKeys(t *testing.T) {
	config := dataserver.NewTestConfig(t.TempDir())
	// Test with multiple shards to ensure correctness across shards
	config.NumShards = 10
	standaloneServer, err := dataserver.NewStandalone(config)
	assert.NoError(t, err)

	client, err := oxia.NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	ctx := context.Background()

	_, _, err = client.Put(ctx, "a", []byte("0"), oxia.SequenceKeysDeltas(1))
	assert.ErrorIs(t, err, oxia.ErrInvalidOptions)

	_, _, err = client.Put(ctx, "a", []byte("0"),
		oxia.SequenceKeysDeltas(1),
		oxia.PartitionKey("x"),
		oxia.ExpectedVersionId(1),
	)
	assert.ErrorIs(t, err, oxia.ErrInvalidOptions)

	key, _, err := client.Put(ctx, "a", []byte("0"),
		oxia.SequenceKeysDeltas(1),
		oxia.PartitionKey("x"),
	)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("a-%020d", 1), key)

	key, _, err = client.Put(ctx, "a", []byte("1"),
		oxia.SequenceKeysDeltas(3),
		oxia.PartitionKey("x"),
	)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("a-%020d", 4), key)

	key, _, err = client.Put(ctx, "a", []byte("2"),
		oxia.SequenceKeysDeltas(1, 6),
		oxia.PartitionKey("x"),
	)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("a-%020d-%020d", 5, 6), key)

	_, _, _, err = client.Get(ctx, "a")
	assert.ErrorIs(t, err, oxia.ErrKeyNotFound)

	_, value, _, err := client.Get(ctx, fmt.Sprintf("a-%020d", 1), oxia.PartitionKey("x"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("0"), value)

	_, value, _, err = client.Get(ctx, fmt.Sprintf("a-%020d", 4), oxia.PartitionKey("x"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("1"), value)

	_, value, _, err = client.Get(ctx, fmt.Sprintf("a-%020d-%020d", 5, 6), oxia.PartitionKey("x"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("2"), value)

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestSyncClientImpl_RangeScan(t *testing.T) {
	config := dataserver.NewTestConfig(t.TempDir())
	// Test with multiple shards to ensure correctness across shards
	config.NumShards = 10
	standaloneServer, err := dataserver.NewStandalone(config)
	assert.NoError(t, err)

	client, err := oxia.NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	ctx := context.Background()

	_, _, _ = client.Put(ctx, "a", []byte("0"))
	_, _, _ = client.Put(ctx, "b", []byte("1"))
	_, _, _ = client.Put(ctx, "c", []byte("2"))
	_, _, _ = client.Put(ctx, "d", []byte("3"))
	_, _, _ = client.Put(ctx, "e", []byte("4"))
	_, _, _ = client.Put(ctx, "f", []byte("5"))
	_, _, _ = client.Put(ctx, "g", []byte("6"))

	ch := client.RangeScan(ctx, "b", "f")

	gr := <-ch
	assert.NoError(t, gr.Err)
	assert.Equal(t, "b", gr.Key)
	assert.Equal(t, []byte("1"), gr.Value)

	gr = <-ch
	assert.NoError(t, gr.Err)
	assert.Equal(t, "c", gr.Key)
	assert.Equal(t, []byte("2"), gr.Value)

	gr = <-ch
	assert.NoError(t, gr.Err)
	assert.Equal(t, "d", gr.Key)
	assert.Equal(t, []byte("3"), gr.Value)

	gr = <-ch
	assert.NoError(t, gr.Err)
	assert.Equal(t, "e", gr.Key)
	assert.Equal(t, []byte("4"), gr.Value)

	_, more := <-ch
	assert.False(t, more)

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestSyncClientImpl_RangeScanOnPartition(t *testing.T) {
	config := dataserver.NewTestConfig(t.TempDir())
	// Test with multiple shards to ensure correctness across shards
	config.NumShards = 10
	standaloneServer, err := dataserver.NewStandalone(config)
	assert.NoError(t, err)

	client, err := oxia.NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	ctx := context.Background()

	_, _, _ = client.Put(ctx, "a", []byte("0"), oxia.PartitionKey("x"))
	_, _, _ = client.Put(ctx, "b", []byte("1"), oxia.PartitionKey("x"))
	_, _, _ = client.Put(ctx, "c", []byte("2"), oxia.PartitionKey("x"))
	_, _, _ = client.Put(ctx, "d", []byte("3"), oxia.PartitionKey("x"))
	_, _, _ = client.Put(ctx, "e", []byte("4"), oxia.PartitionKey("x"))
	_, _, _ = client.Put(ctx, "f", []byte("5"), oxia.PartitionKey("x"))
	_, _, _ = client.Put(ctx, "g", []byte("6"), oxia.PartitionKey("x"))

	ch := client.RangeScan(ctx, "b", "f", oxia.PartitionKey("x"))

	gr := <-ch
	assert.NoError(t, gr.Err)
	assert.Equal(t, "b", gr.Key)
	assert.Equal(t, []byte("1"), gr.Value)

	gr = <-ch
	assert.NoError(t, gr.Err)
	assert.Equal(t, "c", gr.Key)
	assert.Equal(t, []byte("2"), gr.Value)

	gr = <-ch
	assert.NoError(t, gr.Err)
	assert.Equal(t, "d", gr.Key)
	assert.Equal(t, []byte("3"), gr.Value)

	gr = <-ch
	assert.NoError(t, gr.Err)
	assert.Equal(t, "e", gr.Key)
	assert.Equal(t, []byte("4"), gr.Value)

	_, more := <-ch
	assert.False(t, more)

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestAsyncClientImpl_SequenceOrdering(t *testing.T) {
	config := dataserver.NewTestConfig(t.TempDir())
	standaloneServer, err := dataserver.NewStandalone(config)
	assert.NoError(t, err)

	client, err := oxia.NewAsyncClient(standaloneServer.ServiceAddr(), oxia.WithMaxRequestsPerBatch(1))
	assert.NoError(t, err)

	var responses []<-chan oxia.PutResult

	for i := 0; i < 100; i++ {
		r := client.Put("a", []byte("0"), oxia.PartitionKey("x"), oxia.SequenceKeysDeltas(1))
		responses = append(responses, r)
	}

	for i := 0; i < 100; i++ {
		r := <-responses[i]

		assert.Equal(t, fmt.Sprintf("a-%020d", i+1), r.Key)
	}

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestAsyncClientImpl_versionId(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	client, err := oxia.NewAsyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	ch0 := client.Put("/a", []byte("0"))
	ch1 := client.Put("/a", []byte("1"))
	ch2 := client.Put("/a", []byte("2"))

	chb0 := client.Put("/b", []byte("0"))

	r0 := <-ch0
	r1 := <-ch1
	r2 := <-ch2
	rb0 := <-chb0

	assert.NoError(t, r0.Err)
	assert.NoError(t, r1.Err)
	assert.NoError(t, r2.Err)
	assert.NoError(t, rb0.Err)

	assert.EqualValues(t, 0, r0.Version.VersionId)
	assert.EqualValues(t, 1, r1.Version.VersionId)
	assert.EqualValues(t, 2, r2.Version.VersionId)
	assert.EqualValues(t, 3, rb0.Version.VersionId)

	ch3 := client.Put("/a", []byte("3"))
	r3 := <-ch3
	assert.EqualValues(t, 4, r3.Version.VersionId)

	assert.NoError(t, standaloneServer.Close())
}

func TestGetValueWithSessionId(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client, err := oxia.NewAsyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	ch0 := client.Put("/TestGetValueWithSessionId", []byte("0"), oxia.Ephemeral())
	r0 := <-ch0
	assert.NoError(t, r0.Err)
	assert.EqualValues(t, 1, r0.Version.VersionId)

	ch1 := client.Get("/TestGetValueWithSessionId")
	r1 := <-ch1
	assert.NoError(t, r1.Err)
	assert.EqualValues(t, r1.Version.SessionId, r0.Version.SessionId)

	// cleanup
	client.Close()

	client, err = oxia.NewAsyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	ch0 = client.Put("/TestGetValueWithSessionId", []byte("0"), oxia.Ephemeral())
	r0 = <-ch0
	assert.NoError(t, r0.Err)
	assert.EqualValues(t, 3, r0.Version.VersionId)

	ch1 = client.Get("/TestGetValueWithSessionId")
	r1 = <-ch1
	assert.NoError(t, r1.Err)
	assert.NotEqualValues(t, 0, r0.Version.SessionId)
	assert.EqualValues(t, r1.Version.SessionId, r0.Version.SessionId)
}

func TestGetWithoutValue(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client, err := oxia.NewAsyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)
	defer client.Close()

	key := "stream"

	keys := make([]string, 0, 2)

	putResult := <-client.Put(key, []byte("0"), oxia.PartitionKey(key), oxia.SequenceKeysDeltas(1))
	assert.NotNil(t, putResult.Key)
	assert.NoError(t, putResult.Err)
	keys = append(keys, putResult.Key)

	putResult = <-client.Put(key, []byte("1"), oxia.PartitionKey(key), oxia.SequenceKeysDeltas(1))
	assert.NotNil(t, putResult.Key)
	assert.NoError(t, putResult.Err)
	keys = append(keys, putResult.Key)

	for _, subKey := range keys {
		result := <-client.Get(subKey, oxia.PartitionKey(key), oxia.IncludeValue(true))
		assert.NotNil(t, result.Value)
		result = <-client.Get(subKey, oxia.PartitionKey(key), oxia.IncludeValue(false))
		assert.Nil(t, result.Value)
	}

	result := <-client.Get(keys[0], oxia.PartitionKey(key), oxia.IncludeValue(false), oxia.ComparisonHigher())
	assert.Nil(t, result.Value)
	assert.Equal(t, result.Key, keys[1])

	result = <-client.Get(keys[1], oxia.PartitionKey(key), oxia.IncludeValue(false), oxia.ComparisonLower())
	assert.Nil(t, result.Value)
	assert.Equal(t, result.Key, keys[0])
}
