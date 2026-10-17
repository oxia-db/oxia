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

package restore

import (
	"bytes"
	"strings"
	"testing"
	"time"

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

func TestCmdRestoresANamespace(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })
	commons.MockedAdminClient.On("Close").Return(nil)
	asOf := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	commons.MockedAdminClient.On("RestoreNamespace", "orders", asOf, "orders-restored", "s3://b/p").
		Return([]*proto.RestoredShard{{
			Shard:       9,
			HashRange:   &proto.HashRange{Min: 0, Max: 10},
			SourceShard: 1,
			BackupId:    "2026-10-03T11-00-00Z-abcd",
		}}, nil)

	out, err := runCmd("--namespace", "orders", "--as-of", "2026-10-03T12:00:00Z", "--to-namespace", "orders-restored",
		"--from", "s3://b/p")
	require.NoError(t, err)
	assert.Contains(t, out, `"namespace":"orders-restored"`)
	assert.Contains(t, out, `"shard":9`)
	assert.Contains(t, out, `"backupId":"2026-10-03T11-00-00Z-abcd"`)
}

func TestCmdRejectsAnInvalidAsOfTime(t *testing.T) {
	_, err := runCmd("--namespace", "orders", "--as-of", "yesterday", "--to-namespace", "orders-restored")
	assert.ErrorContains(t, err, "invalid --as-of time")
}

func TestCmdReportsTheErrorsOfTheClient(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })
	commons.MockedAdminClient.On("Close").Return(nil)
	// Without a result, the mock answers with its own error
	commons.MockedAdminClient.On("RestoreNamespace", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	_, err := runCmd("--namespace", "orders", "--as-of", "2026-10-03T12:00:00Z", "--to-namespace", "orders-restored")
	assert.ErrorContains(t, err, "failed to restore namespace")
}

func TestCmdReportsAClientThatCannotBeBuilt(t *testing.T) {
	commons.MockedAdminClient = nil
	previous := commons.AdminConfig
	t.Cleanup(func() { commons.AdminConfig = previous })
	commons.AdminConfig.Auth.Token, commons.AdminConfig.Auth.TokenFile = "token", "token-file"

	_, err := runCmd("--namespace", "orders", "--as-of", "2026-10-03T12:00:00Z", "--to-namespace", "orders-restored")
	assert.Error(t, err)
}
