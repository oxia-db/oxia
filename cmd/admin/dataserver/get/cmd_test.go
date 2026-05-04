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
	"gopkg.in/yaml.v3"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

func runCmd(args ...string) (string, error) {
	actual := new(bytes.Buffer)
	root := &cobra.Command{Use: "admin"}
	root.PersistentFlags().StringP("output", "o", "", "Output format. One of: json|yaml|name|table")
	root.AddCommand(newCmd())
	root.SetOut(actual)
	root.SetErr(actual)
	root.SetArgs(append([]string{"get"}, args...))
	err := root.Execute()
	return strings.TrimSpace(actual.String()), err
}

func Test_cmd_getDataServer(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("GetDataServer", serverName).Return(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public1",
			Internal: "internal1",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-1"},
		},
	}, nil)

	out, err := runCmd("-o", "json", serverName)

	assert.NoError(t, err)
	var dataServer proto.DataServer
	assert.NoError(t, json.Unmarshal([]byte(out), &dataServer))
	require.NotNil(t, dataServer.Identity)
	assert.NotNil(t, dataServer.Identity.Name)
	assert.Equal(t, serverName, *dataServer.Identity.Name)
	assert.Equal(t, "public1", dataServer.Identity.GetPublic())
	assert.Equal(t, "internal1", dataServer.Identity.GetInternal())
	require.Equal(t, map[string]string{"rack": "rack-1"}, dataServer.Metadata.GetLabels())
}

func Test_cmd_getDataServersIdentities(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "internal2"
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("ListDataServers").Return([]*proto.DataServer{
		{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName1,
				Public:   "public1",
				Internal: "internal1",
			},
			Metadata: &proto.DataServerMetadata{
				Labels: map[string]string{"rack": "rack-1"},
			},
		},
		{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName2,
				Public:   "public2",
				Internal: "internal2",
			},
			Metadata: &proto.DataServerMetadata{
				Labels: map[string]string{"rack": "rack-2"},
			},
		},
	}, nil)

	out, err := runCmd("-o", "json")

	assert.NoError(t, err)
	var dataServers []proto.DataServer
	assert.NoError(t, json.Unmarshal([]byte(out), &dataServers))
	require.Len(t, dataServers, 2)
	require.NotNil(t, dataServers[0].Identity)
	require.NotNil(t, dataServers[1].Identity)
	require.NotNil(t, dataServers[0].Identity.Name)
	require.NotNil(t, dataServers[1].Identity.Name)
	assert.Equal(t, serverName1, *dataServers[0].Identity.Name)
	assert.Equal(t, serverName2, *dataServers[1].Identity.Name)
	assert.Equal(t, "public1", dataServers[0].Identity.GetPublic())
	assert.Equal(t, "public2", dataServers[1].Identity.GetPublic())
	assert.Equal(t, "internal1", dataServers[0].Identity.GetInternal())
	assert.Equal(t, "internal2", dataServers[1].Identity.GetInternal())
	require.Equal(t, map[string]string{"rack": "rack-1"}, dataServers[0].Metadata.GetLabels())
	require.Equal(t, map[string]string{"rack": "rack-2"}, dataServers[1].Metadata.GetLabels())
}

func Test_cmd_getDataServer_YAML(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("GetDataServer", serverName).Return(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public1",
			Internal: "internal1",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-1"},
		},
	}, nil)

	out, err := runCmd("-o", "yaml", serverName)

	assert.NoError(t, err)
	var dataServer map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(out), &dataServer))
	identity, ok := dataServer["identity"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, serverName, identity["name"])
	assert.Equal(t, "public1", identity["public"])
	assert.Equal(t, "internal1", identity["internal"])
}

func Test_cmd_getDataServersIdentities_Name(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "internal2"
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("ListDataServers").Return([]*proto.DataServer{
		{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName1,
				Public:   "public1",
				Internal: "internal1",
			},
		},
		{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName2,
				Public:   "public2",
				Internal: "internal2",
			},
		},
	}, nil)

	out, err := runCmd("-o", "name")

	assert.NoError(t, err)
	assert.Equal(t, "dataserver/server-1\ndataserver/internal2", out)
}

func Test_cmd_getDataServer_InvalidOutput(t *testing.T) {
	commons.MockedAdminClient = nil

	out, err := runCmd("-o", "xml")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), `unsupported output format "xml"`)
	assert.Contains(t, out, `unsupported output format "xml"`)
}

func Test_cmd_getDataServer_DefaultTable(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("GetDataServer", serverName).Return(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public1",
			Internal: "internal1",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-1"},
		},
	}, nil)

	out, err := runCmd(serverName)

	assert.NoError(t, err)
	assert.Contains(t, out, "NAME")
	assert.Contains(t, out, "PUBLIC")
	assert.Contains(t, out, "INTERNAL")
	assert.Contains(t, out, "LABELS")
	assert.Contains(t, out, "server-1")
	assert.Contains(t, out, "public1")
	assert.Contains(t, out, "internal1")
	assert.Contains(t, out, "rack=rack-1")
}

func Test_cmd_getDataServersIdentities_DefaultTable(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "internal2"
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("ListDataServers").Return([]*proto.DataServer{
		{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName1,
				Public:   "public1",
				Internal: "internal1",
			},
		},
		{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName2,
				Public:   "public2",
				Internal: "internal2",
			},
		},
	}, nil)

	out, err := runCmd()

	assert.NoError(t, err)
	assert.Contains(t, out, "NAME")
	assert.Contains(t, out, "PUBLIC")
	assert.Contains(t, out, "INTERNAL")
	assert.Contains(t, out, "server-1")
	assert.Contains(t, out, "public1")
	assert.Contains(t, out, "internal1")
	assert.Contains(t, out, "internal2")
	assert.Contains(t, out, "public2")
}
