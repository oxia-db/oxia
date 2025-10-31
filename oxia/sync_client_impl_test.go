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

package oxia

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/node"
)

type neverCompleteAsyncClient struct {
}

func (c *neverCompleteAsyncClient) Close() error { return nil }

func (c *neverCompleteAsyncClient) Put(key string, value []byte, options ...PutOption) <-chan PutResult {
	return make(chan PutResult)
}

func (c *neverCompleteAsyncClient) Delete(key string, options ...DeleteOption) <-chan error {
	return make(chan error)
}

func (c *neverCompleteAsyncClient) DeleteRange(minKeyInclusive string, maxKeyExclusive string, options ...DeleteRangeOption) <-chan error {
	return make(chan error)
}

func (c *neverCompleteAsyncClient) Get(key string, options ...GetOption) <-chan GetResult {
	return make(chan GetResult)
}

func (c *neverCompleteAsyncClient) List(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...ListOption) <-chan ListResult {
	panic("not implemented")
}

func (c *neverCompleteAsyncClient) RangeScan(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...RangeScanOption) <-chan GetResult {
	panic("not implemented")
}

func (c *neverCompleteAsyncClient) GetNotifications() (Notifications, error) {
	panic("not implemented")
}

func (c *neverCompleteAsyncClient) GetSequenceUpdates(ctx context.Context, prefixKey string, options ...GetSequenceUpdatesOption) (<-chan string, error) {
	panic("not implemented")
}

func TestCancelContext(t *testing.T) {
	_asyncClient := &neverCompleteAsyncClient{}
	syncClient := newSyncClient(_asyncClient)

	assertCancellable(t, func(ctx context.Context) error {
		_, _, err := syncClient.Put(ctx, "/a", []byte{})
		return err
	})
	assertCancellable(t, func(ctx context.Context) error {
		return syncClient.Delete(ctx, "/a")
	})
	assertCancellable(t, func(ctx context.Context) error {
		return syncClient.DeleteRange(ctx, "/a", "/b")
	})
	assertCancellable(t, func(ctx context.Context) error {
		_, _, _, err := syncClient.Get(ctx, "/a")
		return err
	})

	err := syncClient.Close()
	assert.NoError(t, err)
}

func assertCancellable(t *testing.T, operationFunc func(context.Context) error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error)
	go func() {
		errCh <- operationFunc(ctx)
	}()

	cancel()

	assert.ErrorIs(t, <-errCh, context.Canceled)
}

func TestSyncClientImpl_SecondaryIndexes(t *testing.T) {
	config := node.NewTestConfig(t.TempDir())
	// Test with multiple shards to ensure correctness across shards
	config.NumShards = 1
	standaloneServer, err := node.NewStandalone(config)
	assert.NoError(t, err)

	client, err := NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	// ////////////////////////////////////////////////////////////////////////

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		primKey := fmt.Sprintf("%c", 'a'+i)
		val := fmt.Sprintf("%d", i)
		slog.Info("Adding record",
			slog.String("key", primKey),
			slog.String("value", val),
		)
		_, _, _ = client.Put(ctx, primKey, []byte(val), SecondaryIndex("val-idx", val))
	}

	// ////////////////////////////////////////////////////////////////////////

	l, err := client.List(ctx, "1", "4", UseIndex("val-idx"))
	assert.NoError(t, err)
	assert.Equal(t, []string{"b", "c", "d"}, l)

	// ////////////////////////////////////////////////////////////////////////

	resCh := client.RangeScan(ctx, "1", "4", UseIndex("val-idx"))
	i := 1
	for res := range resCh {
		assert.NoError(t, res.Err)

		primKey := fmt.Sprintf("%c", 'a'+i)
		val := fmt.Sprintf("%d", i)

		slog.Info("Expected record",
			slog.String("expected-key", primKey),
			slog.String("expected-value", val),
			slog.String("received-key", res.Key),
			slog.String("received-value", string(res.Value)),
		)
		assert.Equal(t, primKey, res.Key)
		assert.Equal(t, val, string(res.Value))
		i++
	}

	assert.Equal(t, 4, i)

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestSyncClientImpl_SecondaryIndexesRepeated(t *testing.T) {
	config := node.NewTestConfig(t.TempDir())
	// Test with multiple shards to ensure correctness across shards
	config.NumShards = 1
	standaloneServer, err := node.NewStandalone(config)
	assert.NoError(t, err)

	client, err := NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	// ////////////////////////////////////////////////////////////////////////

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		primKey := fmt.Sprintf("/%c", 'a'+i)
		val := fmt.Sprintf("%c", 'a'+i)
		slog.Info("Adding record",
			slog.String("key", primKey),
			slog.String("value", val),
		)
		_, _, _ = client.Put(ctx, primKey, []byte(val),
			SecondaryIndex("val-idx", val),
			SecondaryIndex("val-idx", strings.ToUpper(val)),
		)
	}

	// ////////////////////////////////////////////////////////////////////////

	l, err := client.List(ctx, "b", "e", UseIndex("val-idx"))
	assert.NoError(t, err)
	assert.Equal(t, []string{"/b", "/c", "/d"}, l)

	l, err = client.List(ctx, "I", "d", UseIndex("val-idx"))
	assert.NoError(t, err)
	assert.Equal(t, []string{"/i", "/j", "/a", "/b", "/c"}, l)

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestSyncClientImpl_SecondaryIndexes_Get(t *testing.T) {
	config := node.NewTestConfig(t.TempDir())
	config.NumShards = 10
	doSecondaryIndexesGet(t, config)
}

func TestSyncClientImpl_SecondaryIndexes_Get_NoNotifications(t *testing.T) {
	config := node.NewTestConfig(t.TempDir())
	config.NotificationsEnabled = false
	doSecondaryIndexesGet(t, config)
}

func doSecondaryIndexesGet(t *testing.T, config node.StandaloneConfig) {
	t.Helper()
	standaloneServer, err := node.NewStandalone(config)
	assert.NoError(t, err)

	client, err := NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	// ////////////////////////////////////////////////////////////////////////

	ctx := context.Background()
	for i := 1; i < 10; i++ {
		primKey := fmt.Sprintf("%c", 'a'+i)
		val := fmt.Sprintf("%03d", i)
		slog.Info("Adding record",
			slog.String("key", primKey),
			slog.String("value", val))
		_, _, _ = client.Put(ctx, primKey, []byte(val), SecondaryIndex("val-idx", val))
	}

	var primaryKey string
	var val []byte
	// ////////////////////////////////////////////////////////////////////////

	_, _, _, err = client.Get(ctx, "000", UseIndex("val-idx"))
	assert.ErrorIs(t, err, ErrKeyNotFound)

	primaryKey, val, _, err = client.Get(ctx, "001", UseIndex("val-idx"))
	assert.NoError(t, err)
	assert.Equal(t, "b", primaryKey)
	assert.Equal(t, []byte("001"), val)

	primaryKey, val, _, err = client.Get(ctx, "005", UseIndex("val-idx"))
	assert.NoError(t, err)
	assert.Equal(t, "f", primaryKey)
	assert.Equal(t, []byte("005"), val)

	primaryKey, val, _, err = client.Get(ctx, "009", UseIndex("val-idx"))
	assert.NoError(t, err)
	assert.Equal(t, "j", primaryKey)
	assert.Equal(t, []byte("009"), val)

	_, _, _, err = client.Get(ctx, "999", UseIndex("val-idx"))
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// ////////////////////////////////////////////////////////////////////////

	_, _, _, err = client.Get(ctx, "000", UseIndex("val-idx"), ComparisonLower())
	assert.ErrorIs(t, err, ErrKeyNotFound)

	_, _, _, err = client.Get(ctx, "001", UseIndex("val-idx"), ComparisonLower())
	assert.ErrorIs(t, err, ErrKeyNotFound)

	primaryKey, val, _, err = client.Get(ctx, "005", UseIndex("val-idx"), ComparisonLower())
	assert.NoError(t, err)
	assert.Equal(t, "e", primaryKey)
	assert.Equal(t, []byte("004"), val)

	primaryKey, val, _, err = client.Get(ctx, "009", UseIndex("val-idx"), ComparisonLower())
	assert.NoError(t, err)
	assert.Equal(t, "i", primaryKey)
	assert.Equal(t, []byte("008"), val)

	primaryKey, val, _, err = client.Get(ctx, "999", UseIndex("val-idx"), ComparisonLower())
	assert.NoError(t, err)
	assert.Equal(t, "j", primaryKey)
	assert.Equal(t, []byte("009"), val)

	// ////////////////////////////////////////////////////////////////////////

	_, _, _, err = client.Get(ctx, "000", UseIndex("val-idx"), ComparisonFloor())
	assert.ErrorIs(t, err, ErrKeyNotFound)

	primaryKey, val, _, err = client.Get(ctx, "001", UseIndex("val-idx"), ComparisonFloor())
	assert.NoError(t, err)
	assert.Equal(t, "b", primaryKey)
	assert.Equal(t, []byte("001"), val)

	primaryKey, val, _, err = client.Get(ctx, "005", UseIndex("val-idx"), ComparisonFloor())
	assert.NoError(t, err)
	assert.Equal(t, "f", primaryKey)
	assert.Equal(t, []byte("005"), val)

	primaryKey, val, _, err = client.Get(ctx, "009", UseIndex("val-idx"), ComparisonFloor())
	assert.NoError(t, err)
	assert.Equal(t, "j", primaryKey)
	assert.Equal(t, []byte("009"), val)

	primaryKey, val, _, err = client.Get(ctx, "999", UseIndex("val-idx"), ComparisonFloor())
	assert.NoError(t, err)
	assert.Equal(t, "j", primaryKey)
	assert.Equal(t, []byte("009"), val)

	// ////////////////////////////////////////////////////////////////////////

	primaryKey, val, _, err = client.Get(ctx, "000", UseIndex("val-idx"), ComparisonHigher())
	assert.NoError(t, err)
	assert.Equal(t, "b", primaryKey)
	assert.Equal(t, []byte("001"), val)

	primaryKey, val, _, err = client.Get(ctx, "001", UseIndex("val-idx"), ComparisonHigher())
	assert.NoError(t, err)
	assert.Equal(t, "c", primaryKey)
	assert.Equal(t, []byte("002"), val)

	primaryKey, val, _, err = client.Get(ctx, "005", UseIndex("val-idx"), ComparisonHigher())
	assert.NoError(t, err)
	assert.Equal(t, "g", primaryKey)
	assert.Equal(t, []byte("006"), val)

	_, _, _, err = client.Get(ctx, "009", UseIndex("val-idx"), ComparisonHigher())
	assert.ErrorIs(t, err, ErrKeyNotFound)

	_, _, _, err = client.Get(ctx, "999", UseIndex("val-idx"), ComparisonHigher())
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// ////////////////////////////////////////////////////////////////////////

	primaryKey, val, _, err = client.Get(ctx, "000", UseIndex("val-idx"), ComparisonCeiling())
	assert.NoError(t, err)
	assert.Equal(t, "b", primaryKey)
	assert.Equal(t, []byte("001"), val)

	primaryKey, val, _, err = client.Get(ctx, "001", UseIndex("val-idx"), ComparisonCeiling())
	assert.NoError(t, err)
	assert.Equal(t, "b", primaryKey)
	assert.Equal(t, []byte("001"), val)

	primaryKey, val, _, err = client.Get(ctx, "005", UseIndex("val-idx"), ComparisonCeiling())
	assert.NoError(t, err)
	assert.Equal(t, "f", primaryKey)
	assert.Equal(t, []byte("005"), val)

	primaryKey, val, _, err = client.Get(ctx, "009", UseIndex("val-idx"), ComparisonCeiling())
	assert.NoError(t, err)
	assert.Equal(t, "j", primaryKey)
	assert.Equal(t, []byte("009"), val)

	_, _, _, err = client.Get(ctx, "999", UseIndex("val-idx"), ComparisonCeiling())
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// ////////////////////////////////////////////////////////////////////////

	assert.NoError(t, client.Close())
	assert.NoError(t, standaloneServer.Close())
}

func TestSyncClientImpl_GetSequenceUpdates(t *testing.T) {
	standaloneServer, err := node.NewStandalone(node.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	client, err := NewSyncClient(standaloneServer.ServiceAddr(), WithBatchLinger(0))
	assert.NoError(t, err)

	ch1, err := client.GetSequenceUpdates(context.Background(), "a")
	assert.Nil(t, ch1)
	assert.ErrorIs(t, err, ErrInvalidOptions)

	ctx1, cancel1 := context.WithCancel(context.Background())
	ch1, err = client.GetSequenceUpdates(ctx1, "a", PartitionKey("x"))
	assert.NotNil(t, ch1)
	assert.NoError(t, err)
	cancel1()

	k1, _, _ := client.Put(context.Background(), "a", []byte("0"), PartitionKey("x"), SequenceKeysDeltas(1))
	assert.Equal(t, fmt.Sprintf("a-%020d", 1), k1)
	k2, _, _ := client.Put(context.Background(), "a", []byte("0"), PartitionKey("x"), SequenceKeysDeltas(1))
	assert.Equal(t, fmt.Sprintf("a-%020d", 2), k2)
	assert.NotEqual(t, k1, k2)

	ctx2, cancel2 := context.WithCancel(context.Background())
	updates2, err := client.GetSequenceUpdates(ctx2, "a", PartitionKey("x"))
	require.NoError(t, err)

	recvK2 := <-updates2
	assert.Equal(t, k2, recvK2)

	cancel2()

	k3, _, _ := client.Put(context.Background(), "a", []byte("0"), PartitionKey("x"), SequenceKeysDeltas(1))
	assert.Empty(t, updates2)

	assert.Eventually(t, func() bool {
		select {
		case <-updates2:
			// Ok
			return true

		default:
			assert.Fail(t, "should have been closed")
			return false
		}
	}, 10*time.Second, 10*time.Millisecond)

	updates3, err := client.GetSequenceUpdates(context.Background(), "a", PartitionKey("x"))
	require.NoError(t, err)

	recvK3 := <-updates3
	assert.Equal(t, k3, recvK3)

	k4, _, _ := client.Put(context.Background(), "a", []byte("0"), PartitionKey("x"), SequenceKeysDeltas(1))
	recvK4 := <-updates3
	assert.Equal(t, k4, recvK4)

	assert.NoError(t, client.Close())

	assert.NoError(t, standaloneServer.Close())
}
