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

package patch

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

func runCmd(cmd *cobra.Command, args ...string) (string, error) {
	actual := new(bytes.Buffer)
	root := &cobra.Command{Use: "admin"}
	root.PersistentFlags().StringP("output", "o", "", "Output format. One of: json|yaml|name|table")
	root.AddCommand(cmd)
	root.SetOut(actual)
	root.SetErr(actual)
	root.SetArgs(append([]string{"patch"}, args...))
	err := root.Execute()
	fields.Reset()
	return strings.TrimSpace(actual.String()), err
}

func Test_cmd_patchNamespace(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	notificationsEnabled := false
	expected := &proto.Namespace{
		Name:                 "ns-1",
		InitialShardCount:    4,
		ReplicationFactor:    2,
		NotificationsEnabled: &notificationsEnabled,
		KeySorting:           "natural",
	}

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("PatchNamespace", mock.MatchedBy(func(namespace *proto.Namespace) bool {
		return namespace.GetName() == "ns-1" &&
			namespace.GetInitialShardCount() == 0 &&
			namespace.GetReplicationFactor() == 2 &&
			namespace.GetNotificationsEnabled() == notificationsEnabled &&
			namespace.GetKeySorting() == ""
	})).Return(expected, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	fields.AddPatchFlags(cmd)
	out, err := runCmd(cmd, "ns-1", "--replication-factor", "2",
		"--notifications=false", "-o", "json")

	require.NoError(t, err)
	var namespace proto.Namespace
	require.NoError(t, json.Unmarshal([]byte(out), &namespace))
	assert.Equal(t, "ns-1", namespace.GetName())
	assert.EqualValues(t, 4, namespace.GetInitialShardCount())
	assert.EqualValues(t, 2, namespace.GetReplicationFactor())
	assert.False(t, namespace.NotificationsEnabledOrDefault())
	assert.Equal(t, "natural", namespace.GetKeySorting())
}

func Test_cmd_patchNamespace_DefaultTable(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("PatchNamespace", mock.Anything).Return(&proto.Namespace{
		Name:              "ns-1",
		InitialShardCount: 4,
		ReplicationFactor: 2,
		KeySorting:        "natural",
	}, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	fields.AddPatchFlags(cmd)
	out, err := runCmd(cmd, "ns-1", "--replication-factor", "2")

	require.NoError(t, err)
	assert.Contains(t, out, "NAME")
	assert.Contains(t, out, "INITIAL_SHARDS")
	assert.Contains(t, out, "REPLICATION_FACTOR")
	assert.Contains(t, out, "NOTIFICATIONS")
	assert.Contains(t, out, "KEY_SORTING")
	assert.Contains(t, out, "ns-1")
	assert.Contains(t, out, "4")
	assert.Contains(t, out, "2")
	assert.Contains(t, out, "true")
	assert.Contains(t, out, "natural")
}

func Test_cmd_patchNamespace_Name(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("PatchNamespace", mock.Anything).Return(&proto.Namespace{
		Name: "ns-1",
	}, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	fields.AddPatchFlags(cmd)
	out, err := runCmd(cmd, "ns-1", "--notifications=false", "-o", "name")

	require.NoError(t, err)
	assert.Equal(t, "namespace/ns-1", out)
}

func Test_cmd_patchNamespace_RejectsEmptyPatch(t *testing.T) {
	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	fields.AddPatchFlags(cmd)
	out, err := runCmd(cmd, "ns-1")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "must specify at least one field to patch")
	assert.Contains(t, out, "must specify at least one field to patch")
}

func Test_cmd_patchNamespace_RejectsInvalidName(t *testing.T) {
	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	fields.AddPatchFlags(cmd)
	out, err := runCmd(cmd, "../ns", "--notifications=false")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid path traversal sequence")
	assert.Contains(t, out, "invalid path traversal sequence")
}

func Test_cmd_patchNamespace_RejectsUnsupportedFlags(t *testing.T) {
	testCases := []struct {
		name string
		flag string
	}{
		{name: "initial shard count", flag: "--initial-shards"},
		{name: "key sorting", flag: "--key-sorting"},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{
				Use:          Cmd.Use,
				Short:        Cmd.Short,
				Long:         Cmd.Long,
				Args:         Cmd.Args,
				RunE:         Cmd.RunE,
				SilenceUsage: Cmd.SilenceUsage,
			}
			fields.AddPatchFlags(cmd)
			out, err := runCmd(cmd, "ns-1", tt.flag, "1")

			require.Error(t, err)
			assert.Contains(t, err.Error(), "unknown flag")
			assert.Contains(t, out, "unknown flag")
		})
	}
}

func Test_cmd_patchNamespace_RejectsInvalidReplicationFactor(t *testing.T) {
	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	fields.AddPatchFlags(cmd)
	out, err := runCmd(cmd, "ns-1", "--replication-factor", "0")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "namespace replication factor must be greater than 0")
	assert.Contains(t, out, "namespace replication factor must be greater than 0")
}
