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

package list

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/cmd/common/clientauth"
	"github.com/oxia-db/oxia/oxia"
)

func runCmd(cmd *cobra.Command, args string, stdin string) (string, error) {
	actual := new(bytes.Buffer)
	cmd.SetIn(bytes.NewBufferString(stdin))
	cmd.SetOut(actual)
	cmd.SetErr(actual)
	cmd.SetArgs(strings.Split(args, " "))
	err := cmd.Execute()
	Config.Reset()
	return strings.TrimSpace(actual.String()), err
}

func TestList_exec(t *testing.T) {
	emptyOptions := []oxia.RangeScanOption{oxia.IncludeValue(false)}

	for _, test := range []struct {
		name               string
		args               string
		expectedParameters []any
	}{
		{"range", "--key-min a --key-max c", []any{"a", "c", emptyOptions}},
		{"short", "-s a -e c", []any{"a", "c", emptyOptions}},
		{"range-no-min", "--key-max c", []any{"", "c", emptyOptions}},
		{"range-no-max", "--key-min a", []any{"a", "", emptyOptions}},
		{"partition-key", "-s a -e c -p xyz", []any{"a", "c", []oxia.RangeScanOption{oxia.IncludeValue(false), oxia.PartitionKey("xyz")}}},
		{"index", "-s a -e c --index idx", []any{"a", "c", []oxia.RangeScanOption{oxia.IncludeValue(false), oxia.UseIndex("idx")}}},
		{"internal-keys", "-s a -e c --internal-keys", []any{"a", "c",
			[]oxia.RangeScanOption{oxia.IncludeValue(false), oxia.ShowInternalKeys(true)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			common.MockedClient = common.NewMockClient()

			ch := make(chan oxia.GetResult, 3)
			ch <- oxia.GetResult{Key: "a"}
			ch <- oxia.GetResult{Key: "b"}
			ch <- oxia.GetResult{Key: "c"}
			close(ch)

			common.MockedClient.On("RangeScan", test.expectedParameters...).Return(ch)
			_, err := runCmd(Cmd, test.args, "")
			assert.NoError(t, err)

			common.MockedClient.AssertExpectations(t)
		})
	}
}

func TestListExecReturnsScanError(t *testing.T) {
	common.MockedClient = common.NewMockClient()
	ch := make(chan oxia.GetResult, 2)
	ch <- oxia.GetResult{Key: "a"}
	ch <- oxia.GetResult{Err: errors.New("scan failed")}
	close(ch)
	common.MockedClient.On("RangeScan", "a", "c", []oxia.RangeScanOption{oxia.IncludeValue(false)}).Return(ch)

	out, err := runCmd(Cmd, "--key-min a --key-max c", "")
	assert.ErrorContains(t, err, "scan failed")
	assert.Contains(t, out, "a")
	common.MockedClient.AssertExpectations(t)
}

func TestExecReturnsNewClientError(t *testing.T) {
	useConflictingClientAuth(t)

	_, err := runCmd(Cmd, "-s a -e b", "")

	assert.ErrorIs(t, err, clientauth.ErrAuthTokenConflict)
}

// useConflictingClientAuth makes the creation of the client fail, with
// conflicting sources of the auth token.
func useConflictingClientAuth(t *testing.T) {
	t.Helper()
	mocked, auth := common.MockedClient, common.Config.Auth
	common.MockedClient = nil
	common.Config.Auth = clientauth.Config{Token: "token", TokenFile: "token-file"}
	t.Cleanup(func() { common.MockedClient, common.Config.Auth = mocked, auth })
}
