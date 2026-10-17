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

package backup

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

func TestCmdRequestsTheBackupOfAShard(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })
	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("RequestBackup", "tenant", mock.MatchedBy(func(shard *int64) bool {
		return shard != nil && *shard == 7
	})).Return(uint32(1), nil)

	out, err := runCmd("--namespace", "tenant", "--shard", "7")
	require.NoError(t, err)
	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &result))
	assert.EqualValues(t, 1, result["shards"])
}

func TestCmdListsTheBackups(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })
	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("ListBackups", "tenant", (*int64)(nil), "").Return([]*proto.BackupInfo{{
		Shard:           3,
		BackupId:        "2026-10-03T14-25-07Z-abcd",
		TimestampMillis: 1791037507000,
		CommitOffset:    42,
		Term:            2,
		HashRange:       &proto.HashRange{Min: 0, Max: 10},
		SizeBytes:       1234,
		SstCount:        5,
	}}, nil)

	out, err := runCmd("list", "--namespace", "tenant")
	require.NoError(t, err)
	var result Result
	require.NoError(t, json.Unmarshal([]byte(out), &result))
	assert.Contains(t, out, `"backupId":"2026-10-03T14-25-07Z-abcd"`)
	assert.Contains(t, out, `"time":"2026-10-03T14:25:07Z"`)
	assert.Contains(t, out, `"commitOffset":42`)
}

func TestCmdReportsTheErrorsOfTheClient(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })
	commons.MockedAdminClient.On("Close").Return(nil)
	// A result of another type is the mock's own error
	commons.MockedAdminClient.On("RequestBackup", "tenant", mock.Anything).Return(nil, nil)
	commons.MockedAdminClient.On("ListBackups", "tenant", mock.MatchedBy(func(shard *int64) bool {
		return shard != nil && *shard == 3
	}), "s3://b").Return(nil, nil)

	_, err := runCmd("--namespace", "tenant")
	assert.ErrorContains(t, err, "failed to request backup")
	_, err = runCmd("list", "--namespace", "tenant", "--shard", "3", "--destination", "s3://b")
	assert.ErrorContains(t, err, "no backups available")
}

func TestCmdReportsAClientThatCannotBeBuilt(t *testing.T) {
	commons.MockedAdminClient = nil
	previous := commons.AdminConfig
	t.Cleanup(func() { commons.AdminConfig = previous })
	commons.AdminConfig.Auth.Token, commons.AdminConfig.Auth.TokenFile = "token", "token-file"

	_, err := runCmd("--namespace", "tenant")
	assert.Error(t, err)
	_, err = runCmd("list", "--namespace", "tenant")
	assert.Error(t, err)
}
