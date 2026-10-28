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

package raft

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/failpoint"
	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

// An applied entry reaches the cache in the background through OnApplied, and
// right away through Reload, which a new leader runs once the entries are
// applied.
func TestProviderCacheFollowsAppliedEntries(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &Raft{logger: logger, sc: newStateContainer(logger, nil)}
	p := NewProvider(t.Context(), r, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled)
	defer func() { require.NoError(t, p.Close()) }()
	r.sc.interceptor = p
	require.Empty(t, loadedValue(t, p).GetInstanceId())

	apply := func(version int64, id string) {
		t.Helper()
		state, err := metadatacodec.ClusterStatusCodec.MarshalJSON(&commonproto.ClusterStatus{InstanceId: id})
		require.NoError(t, err)
		data, err := json.Marshal(raftOpCmd{
			Key:             metadatacodec.ClusterStatusCodec.GetKey(),
			NewState:        state,
			ExpectedVersion: version,
		})
		require.NoError(t, err)
		res, ok := r.sc.Apply(&hashicorpraft.Log{Data: data}).(*applyResult)
		require.True(t, ok)
		require.True(t, res.changeApplied)
	}

	apply(-1, "first")
	require.Eventually(t, func() bool { return loadedValue(t, p).GetInstanceId() == "first" }, 5*time.Second, 10*time.Millisecond)

	// Without the background reload, Reload still loads the applied entry.
	r.sc.interceptor = nil
	apply(0, "second")
	require.NoError(t, p.Reload())
	require.Equal(t, "second", loadedValue(t, p).GetInstanceId())
}

// An entry applied to another document doesn't reload the cache.
func TestProviderOnAppliedIgnoresOtherDocuments(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &Raft{logger: logger, sc: newStateContainer(logger, nil)}
	p := NewProvider(t.Context(), r, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled)
	defer func() { require.NoError(t, p.Close()) }()

	p.OnApplied(metadatacodec.ClusterConfigCodec.GetKey(), nil, 0)
	assert.Empty(t, p.changes)
}

func TestProviderSubscribe(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &Raft{logger: logger, sc: newStateContainer(logger, nil)}
	p := NewProvider(t.Context(), r, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled)
	defer func() { require.NoError(t, p.Close()) }()

	subscription := p.Subscribe()
	current, err := subscription.Get()
	require.NoError(t, err)
	assert.Equal(t, metadatacommon.NotExists, current.Version)
}

func TestProviderLoadLatestPanicsOnUndecodableState(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &Raft{logger: logger, sc: newStateContainer(logger, nil)}
	r.sc.Documents[metadatacodec.ClusterStatusCodec.GetKey()] = &documentContainer{
		State:          json.RawMessage("not json"),
		CurrentVersion: 0,
	}
	p := &Provider[*commonproto.ClusterStatus]{codec: metadatacodec.ClusterStatusCodec, raft: r}

	assert.Panics(t, func() { p.loadLatest() })
}

func TestProviderGetLeaderNameUnavailable(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &Raft{logger: logger, sc: newStateContainer(logger, nil)}
	p := NewProvider(t.Context(), r, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled)
	defer func() { require.NoError(t, p.Close()) }()

	_, err := p.GetLeaderName()
	require.ErrorIs(t, err, errs.ErrUnavailable)
}

func TestProviderStoreFailsOnFollower(t *testing.T) {
	r, _ := newInmemRaft(t)
	p := NewProvider(t.Context(), r, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled)
	defer func() { require.NoError(t, p.Close()) }()

	_, err := p.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   &commonproto.ClusterStatus{InstanceId: "follower"},
		Version: metadatacommon.NotExists,
	})
	require.ErrorIs(t, err, hashicorpraft.ErrNotLeader)
}

func TestProviderStoreOnLeader(t *testing.T) {
	r := newInmemLeader(t)
	p := NewProvider(t.Context(), r, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled)
	defer func() { require.NoError(t, p.Close()) }()

	version, err := p.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   &commonproto.ClusterStatus{InstanceId: "leader"},
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)
	assert.Equal(t, metadatacommon.Version("0"), version)

	// A stale expected version is rejected by the FSM
	_, err = p.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   &commonproto.ClusterStatus{InstanceId: "stale"},
		Version: metadatacommon.NotExists,
	})
	require.ErrorIs(t, err, errs.ErrMetaBadVersion)

	// Undecodable values are refused before reaching raft
	_, err = p.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   &commonproto.ClusterStatus{InstanceId: "\xff"},
		Version: version,
	})
	require.Error(t, err)
	assert.Equal(t, "leader", loadedStatus(t, p).GetInstanceId())
}

func TestProviderStoreFailsWhenStateIsNotJSON(t *testing.T) {
	r := newInmemLeader(t)
	p := NewProvider(t.Context(), r, &fakeCodec{
		Codec:       metadatacodec.ClusterStatusCodec,
		marshalJSON: func(*commonproto.ClusterStatus) ([]byte, error) { return []byte("not json"), nil },
	}, metadatacommon.WatchDisabled)
	defer func() { require.NoError(t, p.Close()) }()

	_, err := p.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   &commonproto.ClusterStatus{InstanceId: "leader"},
		Version: metadatacommon.NotExists,
	})
	var syntaxErr *json.SyntaxError
	require.ErrorAs(t, err, &syntaxErr)
}

func TestProviderStoreFailsWhenApplyFails(t *testing.T) {
	r := newInmemLeader(t)
	p := NewProvider(t.Context(), r, &fakeCodec{
		Codec: metadatacodec.ClusterStatusCodec,
		marshalJSON: func(value *commonproto.ClusterStatus) ([]byte, error) {
			if err := r.node.Shutdown().Error(); err != nil {
				return nil, err
			}
			return metadatacodec.ClusterStatusCodec.MarshalJSON(value)
		},
	}, metadatacommon.WatchDisabled)
	defer func() { require.NoError(t, p.Close()) }()

	_, err := p.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   &commonproto.ClusterStatus{InstanceId: "leader"},
		Version: metadatacommon.NotExists,
	})
	require.ErrorIs(t, err, hashicorpraft.ErrRaftShutdown)
}

func TestProviderStoreFailsWhenStateMachineFails(t *testing.T) {
	skipWithoutFailpoints(t)
	r := newInmemLeader(t)
	r.sc.failpoints = failpoint.New(map[string]func() error{
		"apply": func() error { return errors.New("failpoint") },
	})
	p := NewProvider(t.Context(), r, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled)
	defer func() { require.NoError(t, p.Close()) }()

	_, err := p.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   &commonproto.ClusterStatus{InstanceId: "leader"},
		Version: metadatacommon.NotExists,
	})
	require.ErrorContains(t, err, "failed to apply new cluster state")
}

func loadedValue(t *testing.T, p *Provider[*commonproto.ClusterStatus]) *commonproto.ClusterStatus {
	t.Helper()
	snapshot, err := p.Load()
	require.NoError(t, err)
	return snapshot.Value
}

func loadedStatus(t *testing.T, p provider.Provider[*commonproto.ClusterStatus]) *commonproto.ClusterStatus {
	t.Helper()
	snapshot, err := p.Load()
	require.NoError(t, err)
	return snapshot.Value
}

// newInmemRaft starts a raft node on in-memory stores and transport, with
// timeouts short enough for it to win its election right away once it is
// bootstrapped: until then, it stays a follower. The logger is at debug level,
// which the provider logs its writes at.
func newInmemRaft(t *testing.T) (*Raft, hashicorpraft.Server) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	sc := newStateContainer(logger, nil)
	address, transport := hashicorpraft.NewInmemTransport("")
	config := hashicorpraft.DefaultConfig()
	config.LocalID = hashicorpraft.ServerID(address)
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond
	config.Logger = newHclogAdapter(logger, &slog.LevelVar{})
	store := hashicorpraft.NewInmemStore()
	node, err := hashicorpraft.NewRaft(config, sc, store, store, hashicorpraft.NewInmemSnapshotStore(), transport)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, node.Shutdown().Error()) })

	return &Raft{logger: logger, sc: sc, node: node}, hashicorpraft.Server{ID: config.LocalID, Address: address}
}

// newInmemLeader returns a single node raft, once it leads.
func newInmemLeader(t *testing.T) *Raft {
	t.Helper()

	r, server := newInmemRaft(t)
	require.NoError(t, r.node.BootstrapCluster(hashicorpraft.Configuration{
		Servers: []hashicorpraft.Server{server},
	}).Error())
	require.Eventually(t, func() bool { return r.node.State() == hashicorpraft.Leader },
		10*time.Second, 10*time.Millisecond)
	return r
}

type fakeCodec struct {
	metadatacodec.Codec[*commonproto.ClusterStatus]
	marshalJSON func(value *commonproto.ClusterStatus) ([]byte, error)
}

func (c *fakeCodec) MarshalJSON(value *commonproto.ClusterStatus) ([]byte, error) {
	return c.marshalJSON(value)
}

func skipWithoutFailpoints(t *testing.T) {
	t.Helper()
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
}
