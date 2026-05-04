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
	"github.com/oxia-db/oxia/common/proto"
)

func runCmd(cmd *cobra.Command, args ...string) (string, error) {
	actual := new(bytes.Buffer)
	root := &cobra.Command{Use: "admin"}
	root.PersistentFlags().StringP("output", "o", "", "Output format. One of: json|yaml|name|table")
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
	commons.MockedAdminClient.On("GetNamespace", "ns-1").Return(&proto.Namespace{
		Name:              "ns-1",
		InitialShardCount: 4,
		ReplicationFactor: 3,
		KeySorting:        proto.KeySortingType_NATURAL.String(),
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

	var namespace proto.Namespace
	require.NoError(t, json.Unmarshal([]byte(out), &namespace))
	assert.Equal(t, "ns-1", namespace.GetName())
	assert.EqualValues(t, 4, namespace.GetInitialShardCount())
	assert.EqualValues(t, 3, namespace.GetReplicationFactor())
	assert.Equal(t, proto.KeySortingType_NATURAL.String(), namespace.GetKeySorting())
}

func Test_cmd_getNamespace_Name(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("GetNamespace", "ns-1").Return(&proto.Namespace{
		Name:              "ns-1",
		InitialShardCount: 4,
		ReplicationFactor: 3,
	}, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "-o", "name", "ns-1")
	require.NoError(t, err)
	assert.Equal(t, "namespace/ns-1", out)
}

func Test_cmd_getNamespace_DefaultTable(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("GetNamespace", "ns-1").Return(&proto.Namespace{
		Name:              "ns-1",
		InitialShardCount: 4,
		ReplicationFactor: 3,
		KeySorting:        proto.KeySortingType_NATURAL.String(),
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
	assert.Contains(t, out, "NOTIFICATIONS")
	assert.Contains(t, out, "KEY_SORTING")
	assert.Contains(t, out, "ns-1")
	assert.Contains(t, out, "4")
	assert.Contains(t, out, "3")
	assert.Contains(t, out, "true")
	assert.Contains(t, out, proto.KeySortingType_NATURAL.String())
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
