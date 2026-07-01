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

package get

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	namespacecli "github.com/oxia-db/oxia/cmd/admin/namespace/cli"
	"github.com/oxia-db/oxia/common/proto"
)

func runCmd(cmd *cobra.Command, args ...string) (string, error) {
	actual := new(bytes.Buffer)
	root := &cobra.Command{Use: "admin"}
	root.PersistentFlags().StringP("output", "o", "", "Output format. One of: json|yaml|table")
	root.AddCommand(cmd)
	root.SetOut(actual)
	root.SetErr(actual)
	root.SetArgs(append([]string{"get"}, args...))
	err := root.Execute()
	return strings.TrimSpace(actual.String()), err
}

func Test_cmd_getNamespace(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("GetNamespace", "ns-1").Return(&proto.NamespaceView{
		Namespace: &proto.Namespace{
			Name:              "ns-1",
			InitialShardCount: 4,
			ReplicationFactor: 3,
			KeySorting:        proto.KeySortingType_NATURAL.String(),
		},
		NamespaceStatus: &proto.NamespaceRuntimeStatus{
			Shards: map[int64]*proto.ShardMetadata{
				0: {Status: proto.ShardStatusSteadyState},
			},
		},
	}, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "-o", "json", "ns-1")
	require.NoError(t, err)

	var namespace map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &namespace))
	namespaceConfig := namespace["namespace"].(map[string]any)
	namespaceStatus := namespace["namespace_status"].(map[string]any)
	shards := namespaceStatus["shards"].(map[string]any)
	shard := shards["0"].(map[string]any)
	assert.Equal(t, "ns-1", namespaceConfig["name"])
	assert.EqualValues(t, 4, namespaceConfig["initial_shard_count"])
	assert.EqualValues(t, 3, namespaceConfig["replication_factor"])
	assert.Equal(t, proto.KeySortingType_NATURAL.String(), namespaceConfig["key_sorting"])
	assert.NotContains(t, namespaceStatus, "replication_factor")
	assert.EqualValues(t, proto.ShardStatusSteadyState, shard["status"])
}

func Test_cmd_getNamespaces(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("ListNamespaces").Return([]*proto.NamespaceView{
		{
			Namespace: &proto.Namespace{
				Name:              "ns-1",
				InitialShardCount: 4,
				ReplicationFactor: 3,
			},
		},
		{
			Namespace: &proto.Namespace{
				Name:              "ns-2",
				InitialShardCount: 2,
				ReplicationFactor: 1,
			},
		},
	}, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd)
	require.NoError(t, err)
	assert.Contains(t, out, "NAME")
	assert.Contains(t, out, "INITIAL_SHARDS")
	assert.Contains(t, out, "REPLICATION_FACTOR")
	assert.NotContains(t, out, "CURRENT_SHARDS")
	assert.NotContains(t, out, "NOTIFICATIONS")
	assert.NotContains(t, out, "KEY_SORTING")
	assert.Contains(t, out, "ns-1")
	assert.Contains(t, out, "ns-2")
	assert.Contains(t, out, "4")
	assert.Contains(t, out, "2")
	assert.Contains(t, out, "3")
	assert.Contains(t, out, "1")
}

func Test_cmd_getNamespaces_JSON(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("ListNamespaces").Return([]*proto.NamespaceView{
		{
			Namespace: &proto.Namespace{
				Name:              "ns-1",
				InitialShardCount: 4,
				ReplicationFactor: 3,
			},
			NamespaceStatus: &proto.NamespaceRuntimeStatus{
				Shards: map[int64]*proto.ShardMetadata{
					0: {Status: proto.ShardStatusSteadyState},
				},
			},
		},
	}, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "-o", "json")
	require.NoError(t, err)

	var namespaces []map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &namespaces))
	require.Len(t, namespaces, 1)
	namespaceConfig := namespaces[0]["namespace"].(map[string]any)
	namespaceStatus := namespaces[0]["namespace_status"].(map[string]any)
	shards := namespaceStatus["shards"].(map[string]any)
	assert.Equal(t, "ns-1", namespaceConfig["name"])
	assert.Len(t, shards, 1)
}

func Test_cmd_getNamespace_RejectsNameOutput(t *testing.T) {
	commons.MockedAdminClient = nil
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "-o", "name", "ns-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unsupported output format "name"`)
	assert.Contains(t, out, `unsupported output format "name"`)
}

func Test_cmd_getNamespace_DefaultTable(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("GetNamespace", "ns-1").Return(&proto.NamespaceView{
		Namespace: &proto.Namespace{
			Name:              "ns-1",
			InitialShardCount: 4,
			ReplicationFactor: 3,
			KeySorting:        proto.KeySortingType_NATURAL.String(),
		},
		NamespaceStatus: &proto.NamespaceRuntimeStatus{
			Shards: map[int64]*proto.ShardMetadata{
				0: {Status: proto.ShardStatusSteadyState},
				1: {Status: proto.ShardStatusElection},
			},
		},
	}, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "ns-1")
	require.NoError(t, err)
	assert.Contains(t, out, "NAME")
	assert.Contains(t, out, "INITIAL_SHARDS")
	assert.Contains(t, out, "REPLICATION_FACTOR")
	assert.NotContains(t, out, "CURRENT_SHARDS")
	assert.NotContains(t, out, "NOTIFICATIONS")
	assert.NotContains(t, out, "KEY_SORTING")
	assert.Contains(t, out, "ns-1")
	assert.Contains(t, out, "4")
	assert.Contains(t, out, "3")
}

func Test_cmd_getNamespace_InvalidOutput(t *testing.T) {
	commons.MockedAdminClient = nil

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "-o", "xml", "ns-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unsupported output format "xml"`)
	assert.Contains(t, out, `unsupported output format "xml"`)
}

func TestWriteNamespaceRejectsNilNamespace(t *testing.T) {
	err := namespacecli.WriteNamespace(new(bytes.Buffer), "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "namespace must not be nil")
}
