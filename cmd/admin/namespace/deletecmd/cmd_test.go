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

package deletecmd

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
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
	root.SetArgs(append([]string{"delete"}, args...))
	err := root.Execute()
	return strings.TrimSpace(actual.String()), err
}

func Test_cmd_deleteNamespace(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	notificationsEnabled := false
	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("DeleteNamespace", "ns-1").Return(&proto.Namespace{
		Name:                 "ns-1",
		InitialShardCount:    4,
		ReplicationFactor:    3,
		NotificationsEnabled: &notificationsEnabled,
		KeySorting:           "natural",
	}, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "ns-1", "-o", "json")
	require.NoError(t, err)

	var namespace proto.Namespace
	require.NoError(t, json.Unmarshal([]byte(out), &namespace))
	assert.Equal(t, "ns-1", namespace.GetName())
	assert.EqualValues(t, 4, namespace.GetInitialShardCount())
	assert.EqualValues(t, 3, namespace.GetReplicationFactor())
	assert.False(t, namespace.NotificationsEnabledOrDefault())
	assert.Equal(t, "natural", namespace.GetKeySorting())
}

func Test_cmd_deleteNamespace_Name(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("DeleteNamespace", "ns-1").Return(&proto.Namespace{Name: "ns-1"}, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "ns-1", "-o", "name")
	require.NoError(t, err)
	assert.Equal(t, "namespace/ns-1", out)
}

func Test_cmd_deleteNamespace_RejectsInvalidName(t *testing.T) {
	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "../ns")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid path traversal sequence")
	assert.Contains(t, out, "invalid path traversal sequence")
}

func Test_cmd_deleteNamespace_RejectsEmptyName(t *testing.T) {
	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "   ")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "namespace must not be empty")
	assert.Contains(t, out, "namespace must not be empty")
}
