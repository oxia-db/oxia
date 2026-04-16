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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

func Test_cmd_createDataServer(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() {
		commons.MockedAdminClient = nil
	})

	serverName := "server-4"
	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.
		On("CreateDataServer", mock.MatchedBy(func(dataServerInfo *proto.DataServerInfo) bool {
			if dataServerInfo == nil || dataServerInfo.DataServer == nil || dataServerInfo.DataServer.Name == nil {
				return false
			}
			return *dataServerInfo.DataServer.Name == serverName &&
				dataServerInfo.DataServer.PublicAddress == "public-4:6648" &&
				dataServerInfo.DataServer.InternalAddress == "internal-4:6649" &&
				dataServerInfo.Metadata["rack"] == "a"
		})).
		Return(&proto.DataServerInfo{
			DataServer: &proto.DataServer{
				Name:            &serverName,
				PublicAddress:   "public-4:6648",
				InternalAddress: "internal-4:6649",
			},
			Metadata: map[string]string{
				"rack": "a",
			},
		}, nil)

	out, err := runCmd(
		"--name", serverName,
		"--public-address", "public-4:6648",
		"--internal-address", "internal-4:6649",
		"--labels", "rack=a",
	)

	require.NoError(t, err)

	var dataServerInfo proto.DataServerInfo
	require.NoError(t, json.Unmarshal([]byte(out), &dataServerInfo))
	require.NotNil(t, dataServerInfo.DataServer)
	require.NotNil(t, dataServerInfo.DataServer.Name)
	assert.Equal(t, serverName, *dataServerInfo.DataServer.Name)
	assert.Equal(t, "public-4:6648", dataServerInfo.DataServer.PublicAddress)
	assert.Equal(t, "internal-4:6649", dataServerInfo.DataServer.InternalAddress)
	assert.Equal(t, "a", dataServerInfo.Metadata["rack"])
}

func Test_parseMetadataFlagsRejectsInvalidEntries(t *testing.T) {
	metadata, err := parseMetadataFlags([]string{"rack"})

	assert.Error(t, err)
	assert.Nil(t, metadata)
}
