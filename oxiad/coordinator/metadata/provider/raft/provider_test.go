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
	"io"
	"log/slog"
	"testing"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
)

// An applied entry reaches the cache in the background through OnApplied, and
// right away through Reload, which a new leader runs once the entries are
// applied.
func TestProviderCacheFollowsAppliedEntries(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &Raft{logger: logger, sc: newStateContainer(logger, nil)}
	p, ok := NewProvider(t.Context(), r, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled).(*Provider[*commonproto.ClusterStatus])
	require.True(t, ok)
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

func loadedValue(t *testing.T, p *Provider[*commonproto.ClusterStatus]) *commonproto.ClusterStatus {
	t.Helper()
	snapshot, err := p.Load()
	require.NoError(t, err)
	return snapshot.Value
}
