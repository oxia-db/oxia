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

package raft

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
)

func TestStateContainerApplySupportsV0163StatusLog(t *testing.T) {
	sc := newStateContainer(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	status := &commonproto.ClusterStatus{
		InstanceId: "v0163",
		Namespaces: map[string]*commonproto.NamespaceStatus{
			"default": {},
		},
	}
	statusBytes, err := metadatacodec.ClusterStatusCodec.MarshalJSON(status)
	require.NoError(t, err)

	// v0.16.3 only stored `new_state` and `expected_version`.
	payload := map[string]any{
		"new_state":        json.RawMessage(statusBytes),
		"expected_version": int64(-1),
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	res := sc.Apply(&hashicorpraft.Log{Data: data})
	applyRes, ok := res.(*applyResult)
	require.True(t, ok)
	require.True(t, applyRes.changeApplied)
	require.Equal(t, int64(0), applyRes.newVersion)

	document := sc.document(metadatacodec.ClusterStatusCodec.GetKey())
	decoded, err := metadatacodec.ClusterStatusCodec.UnmarshalJSON(document.State)
	require.NoError(t, err)
	require.Equal(t, status.GetInstanceId(), decoded.GetInstanceId())
}

func TestStateContainerSnapshotIncludesV0163StatusState(t *testing.T) {
	sc := newStateContainer(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	status := &commonproto.ClusterStatus{
		InstanceId: "v0163-snapshot",
		Namespaces: map[string]*commonproto.NamespaceStatus{
			"default": {
				Shards: map[int64]*commonproto.ShardMetadata{
					1: {
						Status: commonproto.ShardStatusSteadyState,
						Split: &commonproto.SplitMetadata{
							Phase: commonproto.SplitPhaseCatchUp,
						},
					},
				},
			},
		},
	}
	statusBytes, err := metadatacodec.ClusterStatusCodec.MarshalJSON(status)
	require.NoError(t, err)

	statusDocument := sc.document(metadatacodec.ClusterStatusCodec.GetKey())
	statusDocument.State = statusBytes
	statusDocument.CurrentVersion = 4

	payload, err := marshalStateContainer(sc)
	require.NoError(t, err)

	var legacySnapshot struct {
		State          json.RawMessage `json:"state"`
		CurrentVersion int64           `json:"current_version"`
	}
	require.NoError(t, json.Unmarshal(payload, &legacySnapshot))
	require.Equal(t, int64(4), legacySnapshot.CurrentVersion)

	decoded, err := metadatacodec.ClusterStatusCodec.UnmarshalJSON(legacySnapshot.State)
	require.NoError(t, err)
	require.Equal(t, "v0163-snapshot", decoded.GetInstanceId())
	require.Equal(t, commonproto.ShardStatusSteadyState, decoded.GetNamespaces()["default"].GetShards()[1].GetStatusOrDefault())
	require.Equal(t, commonproto.SplitPhaseCatchUp, decoded.GetNamespaces()["default"].GetShards()[1].GetSplit().GetPhaseOrDefault())

	var currentSnapshot persistedStateContainer
	require.NoError(t, json.Unmarshal(payload, &currentSnapshot))
	require.Contains(t, currentSnapshot.Documents, metadatacodec.ClusterStatusCodec.GetKey())
}
