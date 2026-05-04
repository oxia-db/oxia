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
	Config.Reset()
	return strings.TrimSpace(actual.String()), err
}

func Test_cmd_patchDataServer(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("PatchDataServer", mock.MatchedBy(func(ds *proto.DataServer) bool {
		return ds.GetNameOrDefault() == serverName &&
			ds.GetIdentity().GetPublic() == "public2" &&
			ds.GetIdentity().GetInternal() == "internal1" &&
			ds.GetMetadata().GetLabels()["rack"] == "rack-2"
	})).Return(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public2",
			Internal: "internal1",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-2"},
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
	cmd.Flags().StringVar(&Config.publicAddress, publicFlagName, "", "Public address for the data server")
	cmd.Flags().StringVar(&Config.internalAddress, internalFlagName, "", "Internal address for the data server")
	cmd.Flags().StringArrayVar(&Config.labels, labelFlagName, nil, "Label to attach to the data server in key=value form")
	_ = cmd.MarkFlagRequired(publicFlagName)
	_ = cmd.MarkFlagRequired(internalFlagName)

	out, err := runCmd(cmd, serverName, "--public", "public2", "--internal", "internal1", "--label", "rack=rack-2", "-o", "json")
	require.NoError(t, err)

	var dataServer proto.DataServer
	require.NoError(t, json.Unmarshal([]byte(out), &dataServer))
	require.NotNil(t, dataServer.Identity)
	require.NotNil(t, dataServer.Identity.Name)
	assert.Equal(t, serverName, *dataServer.Identity.Name)
	assert.Equal(t, "public2", dataServer.Identity.GetPublic())
	assert.Equal(t, "internal1", dataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-2"}, dataServer.GetMetadata().GetLabels())
}

func Test_cmd_patchDataServer_RejectsMissingRequiredFlags(t *testing.T) {
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
	cmd.Flags().StringVar(&Config.publicAddress, publicFlagName, "", "Public address for the data server")
	cmd.Flags().StringVar(&Config.internalAddress, internalFlagName, "", "Internal address for the data server")
	cmd.Flags().StringArrayVar(&Config.labels, labelFlagName, nil, "Label to attach to the data server in key=value form")
	_ = cmd.MarkFlagRequired(publicFlagName)
	_ = cmd.MarkFlagRequired(internalFlagName)

	out, err := runCmd(cmd, "server-1", "--public", "public1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `required flag(s) "internal" not set`)
	assert.Contains(t, out, `required flag(s) "internal" not set`)
}
