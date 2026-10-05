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

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
)

// A read once an entry is applied, like after the leadership barrier, sees
// the applied state, not the state cached before.
func TestProviderLoadSeesAppliedEntry(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &Raft{logger: logger, sc: newStateContainer(logger, nil)}
	p, ok := NewProvider(t.Context(), r, metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled).(*Provider[*commonproto.ClusterStatus])
	require.True(t, ok)
	defer func() { require.NoError(t, p.Close()) }()
	r.sc.interceptor = p
	require.Empty(t, p.Load().Value.GetInstanceId())

	for i, id := range []string{"first", "second", "third"} {
		state, err := metadatacodec.ClusterStatusCodec.MarshalJSON(&commonproto.ClusterStatus{InstanceId: id})
		require.NoError(t, err)
		data, err := json.Marshal(raftOpCmd{
			Key:             metadatacodec.ClusterStatusCodec.GetKey(),
			NewState:        state,
			ExpectedVersion: int64(i) - 1,
		})
		require.NoError(t, err)
		res, ok := r.sc.Apply(&hashicorpraft.Log{Data: data}).(*applyResult)
		require.True(t, ok)
		require.True(t, res.changeApplied)

		require.Equal(t, id, p.Load().Value.GetInstanceId())
	}
}
