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
	return strings.TrimSpace(actual.String()), err
}

func Test_cmd_createDataServer(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	expected := &proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public1",
			Internal: "internal1",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-1"},
		},
	}

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("CreateDataServer", mock.MatchedBy(func(ds *proto.DataServer) bool {
		return ds.GetNameOrDefault() == serverName &&
			ds.GetIdentity().GetPublic() == "public1" &&
			ds.GetIdentity().GetInternal() == "internal1" &&
			ds.GetMetadata().GetLabels()["rack"] == "rack-1"
	})).Return(expected, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	cmd.Flags().String(publicFlagName, "", "Public address for the data server")
	cmd.Flags().String(internalFlagName, "", "Internal address for the data server")
	cmd.Flags().StringArray("label", nil, "Label to attach to the data server in key=value form")
	_ = cmd.MarkFlagRequired(publicFlagName)
	_ = cmd.MarkFlagRequired(internalFlagName)
	out, err := runCmd(cmd, serverName, "--public", "public1", "--internal", "internal1", "--label", "rack=rack-1", "-o", "json")

	assert.NoError(t, err)
	var dataServer proto.DataServer
	assert.NoError(t, json.Unmarshal([]byte(out), &dataServer))
	require.NotNil(t, dataServer.Identity)
	require.NotNil(t, dataServer.Identity.Name)
	assert.Equal(t, serverName, *dataServer.Identity.Name)
	assert.Equal(t, "public1", dataServer.Identity.GetPublic())
	assert.Equal(t, "internal1", dataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, dataServer.GetMetadata().GetLabels())
}

func Test_cmd_createDataServer_DefaultTable(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("CreateDataServer", mock.Anything).Return(&proto.DataServer{
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
	cmd.Flags().String(publicFlagName, "", "Public address for the data server")
	cmd.Flags().String(internalFlagName, "", "Internal address for the data server")
	cmd.Flags().StringArray("label", nil, "Label to attach to the data server in key=value form")
	_ = cmd.MarkFlagRequired(publicFlagName)
	_ = cmd.MarkFlagRequired(internalFlagName)
	out, err := runCmd(cmd, serverName, "--public", "public1", "--internal", "internal1")

	assert.NoError(t, err)
	assert.Contains(t, out, "NAME")
	assert.Contains(t, out, "PUBLIC")
	assert.Contains(t, out, "INTERNAL")
	assert.Contains(t, out, "LABELS")
	assert.Contains(t, out, "server-1")
	assert.Contains(t, out, "public1")
	assert.Contains(t, out, "internal1")
}

func Test_cmd_createDataServer_Name(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("CreateDataServer", mock.Anything).Return(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public1",
			Internal: "internal1",
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
	cmd.Flags().String(publicFlagName, "", "Public address for the data server")
	cmd.Flags().String(internalFlagName, "", "Internal address for the data server")
	cmd.Flags().StringArray("label", nil, "Label to attach to the data server in key=value form")
	_ = cmd.MarkFlagRequired(publicFlagName)
	_ = cmd.MarkFlagRequired(internalFlagName)
	out, err := runCmd(cmd, serverName, "--public", "public1", "--internal", "internal1", "-o", "name")

	assert.NoError(t, err)
	assert.Equal(t, "dataserver/server-1", out)
}

func Test_cmd_createDataServer_InvalidLabel(t *testing.T) {
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
	cmd.Flags().String(publicFlagName, "", "Public address for the data server")
	cmd.Flags().String(internalFlagName, "", "Internal address for the data server")
	cmd.Flags().StringArray("label", nil, "Label to attach to the data server in key=value form")
	_ = cmd.MarkFlagRequired(publicFlagName)
	_ = cmd.MarkFlagRequired(internalFlagName)
	out, err := runCmd(cmd, "server-1", "--public", "public1", "--internal", "internal1", "--label", "rack")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), `invalid entry "rack", expected key=value`)
	assert.Contains(t, out, `invalid entry "rack", expected key=value`)
}

func Test_cmd_createDataServer_RejectsEmptyName(t *testing.T) {
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
	cmd.Flags().String(publicFlagName, "", "Public address for the data server")
	cmd.Flags().String(internalFlagName, "", "Internal address for the data server")
	cmd.Flags().StringArray("label", nil, "Label to attach to the data server in key=value form")
	_ = cmd.MarkFlagRequired(publicFlagName)
	_ = cmd.MarkFlagRequired(internalFlagName)
	out, err := runCmd(cmd, "   ", "--public", "public1", "--internal", "internal1")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "data server name must not be empty")
	assert.Contains(t, out, "data server name must not be empty")
}
