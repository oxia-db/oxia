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

package dataserver

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

func runCmd(args ...string) (string, error) {
	actual := new(bytes.Buffer)
	Cmd.SetOut(actual)
	Cmd.SetErr(actual)
	Cmd.SetArgs(args)
	err := Cmd.Execute()
	return strings.TrimSpace(actual.String()), err
}

func Test_cmd_dataServerList(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "internal2"
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("ListDataServers").Return([]*proto.DataServer{
		{
			Name:            &serverName1,
			PublicAddress:   "public1",
			InternalAddress: "internal1",
			Metadata:        map[string]string{"rack": "rack-1"},
		},
		{
			Name:            &serverName2,
			PublicAddress:   "public2",
			InternalAddress: "internal2",
			Metadata:        map[string]string{"rack": "rack-2"},
		},
	}, nil)

	out, err := runCmd("list")

	assert.NoError(t, err)
	var dataServers []proto.DataServer
	assert.NoError(t, json.Unmarshal([]byte(out), &dataServers))
	assert.Equal(t, 2, len(dataServers))
	assert.Equal(t, "public1", dataServers[0].PublicAddress)
	assert.Equal(t, "public2", dataServers[1].PublicAddress)
	assert.Equal(t, "internal1", dataServers[0].InternalAddress)
	assert.Equal(t, "internal2", dataServers[1].InternalAddress)
	require.NotNil(t, dataServers[0].Name)
	require.NotNil(t, dataServers[1].Name)
	assert.Equal(t, "server-1", *dataServers[0].Name)
	assert.Equal(t, "internal2", *dataServers[1].Name)
	assert.Equal(t, map[string]string{"rack": "rack-1"}, dataServers[0].Metadata)
	assert.Equal(t, map[string]string{"rack": "rack-2"}, dataServers[1].Metadata)
}
