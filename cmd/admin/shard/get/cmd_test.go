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

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

func runCmd(cmd *cobra.Command, args ...string) (string, error) {
	actual := new(bytes.Buffer)
	root := &cobra.Command{Use: "admin"}
	root.PersistentFlags().StringP("output", "o", "", "Output format. One of: json|yaml|table")
	root.AddCommand(cmd)
	root.SetOut(actual)
	root.SetErr(actual)
	root.SetArgs(append([]string{"get"}, args...))
	err := root.Execute()
	return strings.TrimSpace(actual.String()), err
}

func newGetCmd() *cobra.Command {
	namespace = "default"
	cmd := &cobra.Command{
		Use:          Cmd.Use,
		Short:        Cmd.Short,
		Long:         Cmd.Long,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	}
	cmd.Flags().StringVar(&namespace, "namespace", "default", "Namespace of the shard")
	return cmd
}

func Test_cmd_getShards(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("ListShards", "ns-1").Return([]*proto.ShardView{
		{
			Namespace: "ns-1",
			Shard:     1,
			ShardStatus: &proto.ShardMetadata{
				Status: proto.ShardStatusSteadyState,
				Term:   3,
			},
		},
	}, nil)

	out, err := runCmd(newGetCmd(), "--namespace", "ns-1")
	require.NoError(t, err)
	assert.Contains(t, out, "NAMESPACE")
	assert.Contains(t, out, "SHARD")
	assert.Contains(t, out, "STATUS")
	assert.Contains(t, out, "ns-1")
	assert.Contains(t, out, proto.ShardStatusSteadyState.String())
}

func Test_cmd_getShard_JSON(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("GetShard", "ns-1", int64(1)).Return(&proto.ShardView{
		Namespace: "ns-1",
		Shard:     1,
		ShardStatus: &proto.ShardMetadata{
			Status: proto.ShardStatusElection,
			Term:   4,
		},
	}, nil)

	out, err := runCmd(newGetCmd(), "--namespace", "ns-1", "-o", "json", "1")
	require.NoError(t, err)

	var shard map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &shard))
	shardStatus := shard["shard_status"].(map[string]any)
	assert.Equal(t, "ns-1", shard["namespace"])
	assert.EqualValues(t, 1, shard["shard"])
	assert.EqualValues(t, proto.ShardStatusElection, shardStatus["status"])
	assert.EqualValues(t, 4, shardStatus["term"])
}

func Test_cmd_getShard_RejectsNameOutput(t *testing.T) {
	commons.MockedAdminClient = nil
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	out, err := runCmd(newGetCmd(), "--namespace", "ns-1", "-o", "name", "1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unsupported output format "name"`)
	assert.Contains(t, out, `unsupported output format "name"`)
}
