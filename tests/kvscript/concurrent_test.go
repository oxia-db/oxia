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

package kvscript

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
)

// These races check atomic conditional mutations through independent clients.
// The sequential scripts cannot force competing operations to share a version.
func TestKVConcurrentCAS(t *testing.T) {
	for _, topology := range []struct {
		name string
		kind scriptTopology
	}{{"standalone", standaloneTopology}, {"rf3", replicatedTopology}} {
		for _, sorting := range []string{"hierarchical", "natural"} {
			for _, shards := range []uint32{1, 4} {
				t.Run(fmt.Sprintf("%s/%s/shards-%d", topology.name, sorting, shards), func(t *testing.T) {
					keySorting, err := proto.ParseKeySortingType(sorting)
					require.NoError(t, err)
					r := newRunner(t, shards, keySorting, topology.kind)
					clients := []oxia.SyncClient{r.client, r.newClient(t)}
					partition := "cas-partition"
					for _, route := range []casRoute{{name: "default"}, {name: "partition", partition: &partition}} {
						t.Run(route.name, func(t *testing.T) {
							for _, scenario := range []struct {
								name string
								run  func(*testing.T, []oxia.SyncClient, string, casRoute)
							}{{"put-put", casPutPut}, {"put-delete", casPutDelete}, {"create-only", casCreateOnly}} {
								t.Run(scenario.name, func(t *testing.T) {
									for round := range 3 {
										t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
											key := fmt.Sprintf("cas/%s/%s/%d", route.name, scenario.name, round)
											scenario.run(t, clients, key, route)
										})
									}
								})
							}
						})
					}
				})
			}
		}
	}
}

type casRoute struct {
	name      string
	partition *string
}

func (r casRoute) putOptions(options ...oxia.PutOption) []oxia.PutOption {
	if r.partition != nil {
		options = append(options, oxia.PartitionKey(*r.partition))
	}
	return options
}

func (r casRoute) deleteOptions(options ...oxia.DeleteOption) []oxia.DeleteOption {
	if r.partition != nil {
		options = append(options, oxia.PartitionKey(*r.partition))
	}
	return options
}

func (r casRoute) getOptions() []oxia.GetOption {
	if r.partition == nil {
		return nil
	}
	return []oxia.GetOption{oxia.PartitionKey(*r.partition)}
}

type casResult struct {
	key     string
	version oxia.Version
	err     error
}

type casOperation func(context.Context) casResult

// Both workers reach the start barrier before either operation is released.
// Workers only return results; every assertion runs in the test goroutine.
func casRace(t *testing.T, operations [2]casOperation) [2]casResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()
	start := make(chan struct{})
	ready := make(chan struct{}, len(operations))
	type indexedResult struct {
		index  int
		result casResult
	}
	completed := make(chan indexedResult, len(operations))
	for index, operation := range operations {
		go func() {
			ready <- struct{}{}
			select {
			case <-start:
				completed <- indexedResult{index: index, result: operation(ctx)}
			case <-ctx.Done():
				completed <- indexedResult{index: index, result: casResult{err: ctx.Err()}}
			}
		}()
	}
	for range operations {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatalf("waiting for CAS workers: %v", ctx.Err())
		}
	}
	close(start)
	var results [2]casResult
	for range operations {
		select {
		case result := <-completed:
			results[result.index] = result.result
		case <-ctx.Done():
			t.Fatalf("waiting for CAS results: %v", ctx.Err())
		}
	}
	return results
}

func casWinner(t *testing.T, results [2]casResult) int {
	t.Helper()
	winner := -1
	for index, result := range results {
		if result.err == nil {
			require.Equal(t, -1, winner, "both conditional operations succeeded")
			winner = index
		} else {
			require.ErrorIs(t, result.err, oxia.ErrUnexpectedVersionId, "loser %d", index)
		}
	}
	require.NotEqual(t, -1, winner, "neither conditional operation succeeded")
	return winner
}

func casPutPut(t *testing.T, clients []oxia.SyncClient, key string, route casRoute) {
	t.Helper()
	base := casPut(t, clients[0], key, "initial", route, oxia.ExpectedRecordNotExists())
	casAssertRecord(t, clients, key, "initial", base, route)
	values := [2]string{"left", "right"}
	var operations [2]casOperation
	for index := range operations {
		operations[index] = func(ctx context.Context) casResult {
			storedKey, version, err := clients[index].Put(ctx, key, []byte(values[index]),
				route.putOptions(oxia.ExpectedVersionId(base.VersionId))...)
			return casResult{key: storedKey, version: version, err: err}
		}
	}
	results := casRace(t, operations)
	winner := casWinner(t, results)
	updated := results[winner].version
	require.Equal(t, key, results[winner].key)
	require.NotEqual(t, base.VersionId, updated.VersionId)
	require.Equal(t, base.CreatedTimestamp, updated.CreatedTimestamp)
	require.Equal(t, base.ModificationsCount+1, updated.ModificationsCount)
	casAssertRecord(t, clients, key, values[winner], updated, route)
	casRejectStale(t, clients, key, values[winner], updated, route, base.VersionId)
	casDelete(t, clients[1-winner], key, route, updated.VersionId)
	casRecreate(t, clients, key, route, base.VersionId, updated.VersionId)
}

func casPutDelete(t *testing.T, clients []oxia.SyncClient, key string, route casRoute) {
	t.Helper()
	base := casPut(t, clients[0], key, "initial", route, oxia.ExpectedRecordNotExists())
	casAssertRecord(t, clients, key, "initial", base, route)
	results := casRace(t, [2]casOperation{
		func(ctx context.Context) casResult {
			storedKey, version, err := clients[0].Put(ctx, key, []byte("updated"),
				route.putOptions(oxia.ExpectedVersionId(base.VersionId))...)
			return casResult{key: storedKey, version: version, err: err}
		},
		func(ctx context.Context) casResult {
			err := clients[1].Delete(ctx, key, route.deleteOptions(oxia.ExpectedVersionId(base.VersionId))...)
			return casResult{err: err}
		},
	})
	winner := casWinner(t, results)
	stale := []int64{base.VersionId}
	if winner == 0 {
		updated := results[winner].version
		require.Equal(t, key, results[winner].key)
		require.NotEqual(t, base.VersionId, updated.VersionId)
		require.Equal(t, base.CreatedTimestamp, updated.CreatedTimestamp)
		require.Equal(t, base.ModificationsCount+1, updated.ModificationsCount)
		casAssertRecord(t, clients, key, "updated", updated, route)
		casRejectStale(t, clients, key, "updated", updated, route, base.VersionId)
		casDelete(t, clients[1], key, route, updated.VersionId)
		stale = append(stale, updated.VersionId)
	}
	casRecreate(t, clients, key, route, stale...)
}

func casCreateOnly(t *testing.T, clients []oxia.SyncClient, key string, route casRoute) {
	t.Helper()
	casAssertMissing(t, clients, key, route)
	values := [2]string{"left-created", "right-created"}
	var operations [2]casOperation
	for index := range operations {
		operations[index] = func(ctx context.Context) casResult {
			storedKey, version, err := clients[index].Put(ctx, key, []byte(values[index]),
				route.putOptions(oxia.ExpectedRecordNotExists())...)
			return casResult{key: storedKey, version: version, err: err}
		}
	}
	results := casRace(t, operations)
	winner := casWinner(t, results)
	created := results[winner].version
	require.Equal(t, key, results[winner].key)
	require.Zero(t, created.ModificationsCount)
	casAssertRecord(t, clients, key, values[winner], created, route)
	casDelete(t, clients[1-winner], key, route, created.VersionId)
	casRecreate(t, clients, key, route, created.VersionId)
}

func casPut(t *testing.T, client oxia.SyncClient, key, value string, route casRoute,
	options ...oxia.PutOption) oxia.Version {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()
	storedKey, version, err := client.Put(ctx, key, []byte(value), route.putOptions(options...)...)
	require.NoError(t, err)
	require.Equal(t, key, storedKey)
	return version
}

func casDelete(t *testing.T, client oxia.SyncClient, key string, route casRoute, version int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()
	require.NoError(t, client.Delete(ctx, key, route.deleteOptions(oxia.ExpectedVersionId(version))...))
}

func casAssertRecord(t *testing.T, clients []oxia.SyncClient, key, value string, version oxia.Version, route casRoute) {
	t.Helper()
	require.False(t, version.Ephemeral)
	require.Zero(t, version.SessionId)
	require.Empty(t, version.ClientIdentity)
	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()
	for index, client := range clients {
		storedKey, storedValue, storedVersion, err := client.Get(ctx, key, route.getOptions()...)
		require.NoError(t, err, "reader %d", index)
		require.Equal(t, key, storedKey)
		require.Equal(t, []byte(value), storedValue)
		require.Equal(t, version, storedVersion)
	}
}

func casAssertMissing(t *testing.T, clients []oxia.SyncClient, key string, route casRoute) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()
	for index, client := range clients {
		_, _, _, err := client.Get(ctx, key, route.getOptions()...)
		require.ErrorIs(t, err, oxia.ErrKeyNotFound, "reader %d", index)
	}
}

func casRejectStale(t *testing.T, clients []oxia.SyncClient, key, value string, version oxia.Version,
	route casRoute, stale ...int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()
	for _, oldVersion := range stale {
		_, _, err := clients[0].Put(ctx, key, []byte("stale-write"),
			route.putOptions(oxia.ExpectedVersionId(oldVersion))...)
		require.ErrorIs(t, err, oxia.ErrUnexpectedVersionId)
		err = clients[1].Delete(ctx, key, route.deleteOptions(oxia.ExpectedVersionId(oldVersion))...)
		require.ErrorIs(t, err, oxia.ErrUnexpectedVersionId)
		casAssertRecord(t, clients, key, value, version, route)
	}
}

func casRecreate(t *testing.T, clients []oxia.SyncClient, key string, route casRoute, stale ...int64) {
	t.Helper()
	casAssertMissing(t, clients, key, route)
	recreated := casPut(t, clients[1], key, "recreated", route, oxia.ExpectedRecordNotExists())
	require.Zero(t, recreated.ModificationsCount)
	for _, oldVersion := range stale {
		require.NotEqual(t, oldVersion, recreated.VersionId)
	}
	casAssertRecord(t, clients, key, "recreated", recreated, route)
	casRejectStale(t, clients, key, "recreated", recreated, route, stale...)
}
