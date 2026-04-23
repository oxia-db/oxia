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

func Test_cmd_getDataServer(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("GetDataServer", serverName).Return(&proto.DataServerInfo{
		DataServer: &proto.DataServer{
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
	var dataServer proto.DataServerInfo
	assert.NoError(t, json.Unmarshal([]byte(out), &dataServer))
	require.NotNil(t, dataServer.DataServer)
	assert.NotNil(t, dataServer.DataServer.Name)
	assert.Equal(t, serverName, *dataServer.DataServer.Name)
	assert.Equal(t, "public1", dataServer.DataServer.GetPublic())
	assert.Equal(t, "internal1", dataServer.DataServer.GetInternal())
	require.Equal(t, map[string]string{"rack": "rack-1"}, dataServer.Metadata.GetLabels())
}
