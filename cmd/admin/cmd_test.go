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

package admin

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/oxia"
)

func runCmd(args ...string) (string, error) {
	actual := new(bytes.Buffer)
	Cmd.SetOut(actual)
	Cmd.SetErr(actual)
	Cmd.SetArgs(args)
	err := Cmd.Execute()
	return strings.TrimSpace(actual.String()), err
}

func Test_cmd_adminShardSplit(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On(
		"SplitShard",
		"tenant",
		int64(7),
		mock.MatchedBy(func(splitPoint *uint32) bool {
			return splitPoint != nil && *splitPoint == 512
		}),
	).Return(&oxia.SplitShardResult{
		LeftChildShardId:  8,
		RightChildShardId: 9,
	})

	out, err := runCmd("shard", "split", "--namespace", "tenant", "--shard", "7", "--split-point", "512")
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &result))
	assert.EqualValues(t, 8, result["LeftChildShardId"])
	assert.EqualValues(t, 9, result["RightChildShardId"])
	assert.Nil(t, result["Error"])
	commons.MockedAdminClient.AssertExpectations(t)
}
