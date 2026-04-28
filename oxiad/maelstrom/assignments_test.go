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

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

func TestShardAssignmentsLeader(t *testing.T) {
	tests := []struct {
		name        string
		assignments *proto.ShardAssignments
		wantLeader  string
		wantOK      bool
	}{
		{
			name:        "nil assignments",
			assignments: nil,
		},
		{
			name:        "missing default namespace",
			assignments: &proto.ShardAssignments{Namespaces: map[string]*proto.NamespaceShardsAssignment{}},
		},
		{
			name: "empty default namespace assignments",
			assignments: &proto.ShardAssignments{
				Namespaces: map[string]*proto.NamespaceShardsAssignment{
					constant.DefaultNamespace: {},
				},
			},
		},
		{
			name: "valid leader",
			assignments: &proto.ShardAssignments{
				Namespaces: map[string]*proto.NamespaceShardsAssignment{
					constant.DefaultNamespace: {
						Assignments: []*proto.ShardAssignment{
							{Leader: "n1"},
						},
					},
				},
			},
			wantLeader: "n1",
			wantOK:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLeader, gotOK := getDefaultNamespaceLeader(tt.assignments)
			require.Equal(t, tt.wantLeader, gotLeader)
			require.Equal(t, tt.wantOK, gotOK)
		})
	}
}

func TestMaelstromShardAssignmentClientSendSkipsEmptyAssignments(t *testing.T) {
	previousStdout := os.Stdout
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	defer reader.Close()
	defer writer.Close()
	os.Stdout = writer
	defer func() {
		os.Stdout = previousStdout
	}()

	provider := &maelstromCoordinatorRpcProvider{
		dispatcher: &dispatcher{currentLeader: "existing"},
	}
	client := &maelstromShardAssignmentClient{
		provider: provider,
		node:     "n1",
		streamId: 1,
	}

	require.NotPanics(t, func() {
		err := client.Send(&proto.ShardAssignments{Namespaces: map[string]*proto.NamespaceShardsAssignment{}})
		require.NoError(t, err)
	})
	require.Equal(t, "existing", provider.dispatcher.currentLeader)
}

func TestDispatcherSkipsEmptyAssignmentLeaderUpdate(t *testing.T) {
	d := &dispatcher{currentLeader: "existing"}
	require.NotPanics(t, func() {
		d.onOxiaStreamRequestMessage(
			MsgTypeShardAssignmentsResponse,
			nil,
			&proto.ShardAssignments{Namespaces: map[string]*proto.NamespaceShardsAssignment{}},
		)
	})
	require.Equal(t, "existing", d.currentLeader)
}
