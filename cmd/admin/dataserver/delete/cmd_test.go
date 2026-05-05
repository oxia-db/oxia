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

//nolint:revive // delete is a Go keyword, so this package keeps the command suffix.
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

func Test_cmd_deleteDataServer(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("DeleteDataServer", serverName).Return(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public1",
			Internal: "internal1",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-1"},
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
	out, err := runCmd(cmd, serverName, "-o", "json")
	require.NoError(t, err)

	var dataServer proto.DataServer
	require.NoError(t, json.Unmarshal([]byte(out), &dataServer))
	require.NotNil(t, dataServer.Identity)
	require.NotNil(t, dataServer.Identity.Name)
	assert.Equal(t, serverName, *dataServer.Identity.Name)
	assert.Equal(t, "public1", dataServer.Identity.GetPublic())
	assert.Equal(t, "internal1", dataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, dataServer.GetMetadata().GetLabels())
}

func Test_cmd_deleteDataServer_RejectsEmptyName(t *testing.T) {
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
	out, err := runCmd(cmd, "   ")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "data server name must not be empty")
	assert.Contains(t, out, "data server name must not be empty")
}
