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

func dataServerView(dataServer *proto.DataServer) *proto.DataServerView {
	return &proto.DataServerView{
		DataServer: dataServer,
		DataServerStatus: &proto.DataServerStatus{
			State: proto.DataServerState_DATA_SERVER_STATE_RUNNING,
		},
	}
}

func requireJSONMap(t *testing.T, value any) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	require.True(t, ok)
	return result
}

func Test_cmd_getDataServer(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("GetDataServer", serverName).Return(dataServerView(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public1",
			Internal: "internal1",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-1"},
		},
	}), nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "-o", "json", serverName)

	assert.NoError(t, err)
	var dataServer map[string]any
	assert.NoError(t, json.Unmarshal([]byte(out), &dataServer))
	ds := requireJSONMap(t, dataServer["data_server"])
	identity := requireJSONMap(t, ds["identity"])
	metadata := requireJSONMap(t, ds["metadata"])
	labels := requireJSONMap(t, metadata["labels"])
	status := requireJSONMap(t, dataServer["data_server_status"])
	assert.Equal(t, serverName, identity["name"])
	assert.Equal(t, "public1", identity["public"])
	assert.Equal(t, "internal1", identity["internal"])
	assert.Equal(t, "rack-1", labels["rack"])
	assert.EqualValues(t, proto.DataServerState_DATA_SERVER_STATE_RUNNING, status["state"])
}

func Test_cmd_getDataServersIdentities(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "internal2"
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("ListDataServers").Return([]*proto.DataServerView{
		dataServerView(&proto.DataServer{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName1,
				Public:   "public1",
				Internal: "internal1",
			},
			Metadata: &proto.DataServerMetadata{
				Labels: map[string]string{"rack": "rack-1"},
			},
		}),
		dataServerView(&proto.DataServer{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName2,
				Public:   "public2",
				Internal: "internal2",
			},
			Metadata: &proto.DataServerMetadata{
				Labels: map[string]string{"rack": "rack-2"},
			},
		}),
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

	assert.NoError(t, err)
	var dataServers []map[string]any
	assert.NoError(t, json.Unmarshal([]byte(out), &dataServers))
	require.Len(t, dataServers, 2)
	dataServer1 := requireJSONMap(t, dataServers[0]["data_server"])
	dataServer2 := requireJSONMap(t, dataServers[1]["data_server"])
	identity1 := requireJSONMap(t, dataServer1["identity"])
	identity2 := requireJSONMap(t, dataServer2["identity"])
	labels1 := requireJSONMap(t, requireJSONMap(t, dataServer1["metadata"])["labels"])
	labels2 := requireJSONMap(t, requireJSONMap(t, dataServer2["metadata"])["labels"])
	status1 := requireJSONMap(t, dataServers[0]["data_server_status"])
	status2 := requireJSONMap(t, dataServers[1]["data_server_status"])
	assert.Equal(t, serverName1, identity1["name"])
	assert.Equal(t, serverName2, identity2["name"])
	assert.Equal(t, "public1", identity1["public"])
	assert.Equal(t, "public2", identity2["public"])
	assert.Equal(t, "internal1", identity1["internal"])
	assert.Equal(t, "internal2", identity2["internal"])
	assert.Equal(t, "rack-1", labels1["rack"])
	assert.Equal(t, "rack-2", labels2["rack"])
	assert.EqualValues(t, proto.DataServerState_DATA_SERVER_STATE_RUNNING, status1["state"])
	assert.EqualValues(t, proto.DataServerState_DATA_SERVER_STATE_RUNNING, status2["state"])
}

func Test_cmd_getDataServer_YAML(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("GetDataServer", serverName).Return(dataServerView(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public1",
			Internal: "internal1",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-1"},
		},
	}), nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "-o", "yaml", serverName)

	assert.NoError(t, err)
	var dataServer map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(out), &dataServer))
	ds, ok := dataServer["dataserver"].(map[string]any)
	require.True(t, ok)
	identity, ok := ds["identity"].(map[string]any)
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
	commons.MockedAdminClient.On("ListDataServers").Return([]*proto.DataServerView{
		dataServerView(&proto.DataServer{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName1,
				Public:   "public1",
				Internal: "internal1",
			},
		}),
		dataServerView(&proto.DataServer{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName2,
				Public:   "public2",
				Internal: "internal2",
			},
		}),
	}, nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "-o", "name")

	assert.NoError(t, err)
	assert.Equal(t, "dataserver/server-1\ndataserver/internal2", out)
}

func Test_cmd_getDataServer_InvalidOutput(t *testing.T) {
	commons.MockedAdminClient = nil

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, "-o", "xml")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), `unsupported output format "xml"`)
	assert.Contains(t, out, `unsupported output format "xml"`)
}

func Test_cmd_getDataServer_DefaultTable(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("GetDataServer", serverName).Return(dataServerView(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public1",
			Internal: "internal1",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-1"},
		},
	}), nil)

	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	out, err := runCmd(cmd, serverName)

	assert.NoError(t, err)
	assert.Contains(t, out, "NAME")
	assert.Contains(t, out, "PUBLIC")
	assert.Contains(t, out, "INTERNAL")
	assert.Contains(t, out, "STATE")
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
	commons.MockedAdminClient.On("ListDataServers").Return([]*proto.DataServerView{
		dataServerView(&proto.DataServer{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName1,
				Public:   "public1",
				Internal: "internal1",
			},
		}),
		dataServerView(&proto.DataServer{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName2,
				Public:   "public2",
				Internal: "internal2",
			},
		}),
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

	assert.NoError(t, err)
	assert.Contains(t, out, "NAME")
	assert.Contains(t, out, "PUBLIC")
	assert.Contains(t, out, "INTERNAL")
	assert.Contains(t, out, "STATE")
	assert.Contains(t, out, "server-1")
	assert.Contains(t, out, "public1")
	assert.Contains(t, out, "internal1")
	assert.Contains(t, out, "internal2")
	assert.Contains(t, out, "public2")
}
