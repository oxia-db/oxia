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

package create

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
	root.SetArgs(append([]string{"create"}, args...))
	err := root.Execute()
	fields.Reset()
	return strings.TrimSpace(actual.String()), err
}

func Test_cmd_createNamespace(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	notificationsEnabled := false
	expected := &proto.Namespace{
		Name:   "ns-1",
		Policy: proto.NewHierarchyPolicies(4, 3, notificationsEnabled, "natural"),
	}

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("CreateNamespace", mock.MatchedBy(func(namespace *proto.Namespace) bool {
		policy := namespace.GetPolicy()
		return namespace.GetName() == "ns-1" &&
			policy.GetInitialShardCount() == 4 &&
			policy.GetReplicationFactor() == 3 &&
			policy.GetNotificationsEnabled() == notificationsEnabled &&
			policy.GetKeySorting() == "natural"
	})).Return(expected, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	fields.AddFlags(cmd)
	out, err := runCmd(cmd, "ns-1", "--initial-shards", "4", "--replication-factor", "3",
		"--notifications=false", "--key-sorting", "natural", "-o", "json")

	require.NoError(t, err)
	var namespace proto.Namespace
	require.NoError(t, json.Unmarshal([]byte(out), &namespace))
	policy := proto.ResolveHierarchyPolicies(nil, &namespace)
	assert.Equal(t, "ns-1", namespace.GetName())
	assert.EqualValues(t, 4, policy.GetInitialShardCount())
	assert.EqualValues(t, 3, policy.GetReplicationFactor())
	assert.False(t, policy.GetNotificationsEnabled())
	assert.Equal(t, "natural", policy.GetKeySorting())
}

func Test_cmd_createNamespace_DefaultTable(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("CreateNamespace", mock.Anything).Return(&proto.Namespace{
		Name:   "ns-1",
		Policy: proto.NewHierarchyPolicies(4, 3, true, "hierarchical"),
	}, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	fields.AddFlags(cmd)
	out, err := runCmd(cmd, "ns-1", "--initial-shards", "4", "--replication-factor", "3")

	require.NoError(t, err)
	assert.Contains(t, out, "NAME")
	assert.Contains(t, out, "INITIAL_SHARDS")
	assert.Contains(t, out, "REPLICATION_FACTOR")
	assert.Contains(t, out, "NOTIFICATIONS")
	assert.Contains(t, out, "KEY_SORTING")
	assert.Contains(t, out, "ns-1")
	assert.Contains(t, out, "4")
	assert.Contains(t, out, "3")
	assert.Contains(t, out, "true")
	assert.Contains(t, out, "hierarchical")
}

func Test_cmd_createNamespace_Name(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("CreateNamespace", mock.Anything).Return(&proto.Namespace{
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
	fields.AddFlags(cmd)
	out, err := runCmd(cmd, "ns-1", "-o", "name")

	require.NoError(t, err)
	assert.Equal(t, "namespace/ns-1", out)
}

func Test_cmd_createNamespace_RejectsInvalidName(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	fields.AddFlags(cmd)
	out, err := runCmd(cmd, "../ns", "--initial-shards", "4", "--replication-factor", "3")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid path traversal sequence")
	assert.Contains(t, out, "invalid path traversal sequence")
}

func Test_cmd_createNamespace_RejectsInvalidKeySorting(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	fields.AddFlags(cmd)
	out, err := runCmd(cmd, "ns-1", "--initial-shards", "4", "--replication-factor", "3", "--key-sorting", "unknown")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `key sorting must be one of "natural" or "hierarchical"`)
	assert.Contains(t, out, `key sorting must be one of "natural" or "hierarchical"`)
}
