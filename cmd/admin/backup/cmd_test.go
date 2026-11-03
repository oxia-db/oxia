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

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/admin/backup/list"
	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

func runCmd(args ...string) (string, error) {
	// The commands are shared by the runs: the output flag merged into them
	// by an earlier root is reset
	for _, c := range []*cobra.Command{Cmd, list.Cmd} {
		if f := c.Flags().Lookup("output"); f != nil {
			_ = f.Value.Set("")
			f.Changed = false
		}
	}
	actual := new(bytes.Buffer)
	root := &cobra.Command{Use: "admin"}
	root.PersistentFlags().StringP("output", "o", "", "Output format. One of: json|yaml|table")
	root.AddCommand(Cmd)
	root.SetOut(actual)
	root.SetErr(actual)
	root.SetArgs(append([]string{"backup"}, args...))
	err := root.Execute()
	return strings.TrimSpace(actual.String()), err
}

func TestCmdRequestsTheBackupOfAShard(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })
	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("RequestBackup", "tenant", mock.MatchedBy(func(shard *int64) bool {
		return shard != nil && *shard == 7
	})).Return(uint32(1), nil)

	out, err := runCmd("--namespace", "tenant", "--shard", "7", "-o", "json")
	require.NoError(t, err)
	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &result))
	assert.EqualValues(t, 1, result["shards"])
	out, err = runCmd("--namespace", "tenant", "--shard", "7")
	require.NoError(t, err)
	assert.Equal(t, "SHARDS\n1", out, "a table unless asked otherwise")
	_, err = runCmd("--namespace", "tenant", "--shard", "7", "-o", "xml")
	assert.ErrorContains(t, err, "unsupported output format")
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
	}, {
		Shard:           3,
		BackupId:        "2026-10-03T14-25-08Z-abcd",
		TimestampMillis: 1791037507900,
		HashRange:       &proto.HashRange{Min: 0, Max: 10},
	}}, nil)

	out, err := runCmd("list", "--namespace", "tenant", "-o", "json")
	require.NoError(t, err)
	var result Result
	require.NoError(t, json.Unmarshal([]byte(out), &result))
	assert.Contains(t, out, `"backupId": "2026-10-03T14-25-07Z-abcd"`)
	assert.Contains(t, out, `"time": "2026-10-03T14:25:07Z"`)
	assert.Contains(t, out, `"time": "2026-10-03T14:25:07.9Z"`, "the milliseconds of a start are kept: a restore as of that time includes the backup")
	assert.Contains(t, out, `"commitOffset": 42`)
	out, err = runCmd("list", "--namespace", "tenant", "-o", "yaml")
	require.NoError(t, err)
	assert.Contains(t, out, "backupId: 2026-10-03T14-25-07Z-abcd")
	out, err = runCmd("list", "--namespace", "tenant")
	require.NoError(t, err)
	assert.Contains(t, out, "BACKUP ID", "a table unless asked otherwise")
	assert.Contains(t, out, "2026-10-03T14-25-07Z-abcd")
	_, err = runCmd("list", "--namespace", "tenant", "-o", "xml")
	assert.ErrorContains(t, err, "unsupported output format")
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

func TestCmdNeedsTheOutputFlagOfTheAdminCommand(t *testing.T) {
	for name, original := range map[string]*cobra.Command{"backup": Cmd, "list": list.Cmd} {
		t.Run(name, func(t *testing.T) {
			// A copy without the flags merged by the admin command
			bare := &cobra.Command{Use: original.Use, RunE: original.RunE, SilenceUsage: true}
			bare.SetOut(new(bytes.Buffer))
			bare.SetErr(new(bytes.Buffer))
			bare.SetArgs(nil)
			assert.ErrorContains(t, bare.Execute(), "flag accessed but not defined: output")
		})
	}
}
