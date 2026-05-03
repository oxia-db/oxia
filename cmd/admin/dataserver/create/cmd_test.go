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
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

func runCmd(cmd *cobra.Command, args ...string) (string, error) {
	actual := new(bytes.Buffer)
	cmd.SetOut(actual)
	cmd.SetErr(actual)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return strings.TrimSpace(actual.String()), err
}

func TestCmdCreateDataServer(t *testing.T) {
	serverName := "server-1"
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() {
		commons.MockedAdminClient = nil
	})

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("CreateDataServer", &proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public-1:6648",
			Internal: "internal-1:6648",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{
				"rack": "rack-1",
				"zone": "zone-1",
			},
		},
	}).Return(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public-1:6648",
			Internal: "internal-1:6648",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{
				"rack": "rack-1",
				"zone": "zone-1",
			},
		},
	}, nil)

	out, err := runCmd(Cmd,
		serverName,
		"--public", "public-1:6648",
		"--internal", "internal-1:6648",
		"--label", "rack=rack-1",
		"--label", "zone=zone-1",
		"-o", "json",
	)
	require.NoError(t, err)

	var dataServer proto.DataServer
	require.NoError(t, json.Unmarshal([]byte(out), &dataServer))
	require.NotNil(t, dataServer.Identity)
	require.NotNil(t, dataServer.Identity.Name)
	assert.Equal(t, serverName, *dataServer.Identity.Name)
	assert.Equal(t, "public-1:6648", dataServer.Identity.GetPublic())
	assert.Equal(t, "internal-1:6648", dataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{
		"rack": "rack-1",
		"zone": "zone-1",
	}, dataServer.Metadata.GetLabels())
}

func TestParseLabelsRejectsInvalidValue(t *testing.T) {
	_, err := parseLabels([]string{"rack"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "expected key=value")
}
