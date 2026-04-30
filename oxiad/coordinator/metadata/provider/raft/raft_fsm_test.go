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
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
)

func TestStateContainerApplySupportsV0163StatusLog(t *testing.T) {
	sc := newStateContainer(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	status := &commonproto.ClusterStatus{
		InstanceId: "v0163",
		Namespaces: map[string]*commonproto.NamespaceStatus{
			"default": {},
		},
	}
	statusBytes, err := metadatacommon.ClusterStatusCodec.MarshalJSON(status)
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

	document := sc.document(metadatacommon.ClusterStatusCodec.GetKey())
	decoded, err := metadatacommon.ClusterStatusCodec.UnmarshalJSON(document.State)
	require.NoError(t, err)
	require.Equal(t, status.GetInstanceId(), decoded.GetInstanceId())
}
