// Copyright 2023-2025 The Oxia Authors
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

package listnodes

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/oxia"
)

func runCmd(cmd *cobra.Command) (string, error) {
	actual := new(bytes.Buffer)
	cmd.SetOut(actual)
	cmd.SetErr(actual)
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	return strings.TrimSpace(actual.String()), err
}

func Test_cmd_listnodes(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("ListNodes").Return(&oxia.ListNodesResult{
		Nodes: []*oxia.Node{
			{
				Name:            nil,
				PublicAddress:   "public1",
				InternalAddress: "internal1",
			},
			{
				Name:            nil,
				PublicAddress:   "public2",
				InternalAddress: "internal2",
			},
		},
		Error: nil,
	})
	out, err := runCmd(Cmd)

	assert.NoError(t, err)
	var nodes []oxia.Node
	assert.NoError(t, json.Unmarshal([]byte(out), &nodes))
	assert.Equal(t, 2, len(nodes))
	assert.Equal(t, "public1", nodes[0].PublicAddress)
	assert.Equal(t, "public2", nodes[1].PublicAddress)
	assert.Equal(t, "internal1", nodes[0].InternalAddress)
	assert.Equal(t, "internal2", nodes[1].InternalAddress)
	assert.Nil(t, nodes[0].Name)
	assert.Nil(t, nodes[1].Name)
}
