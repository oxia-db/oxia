// Copyright 2023 StreamNative, Inc.
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

package deleterange

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/cmd/client/common"
	"github.com/streamnative/oxia/oxia"
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

func TestDeleteRange_exec(t *testing.T) {
	var emptyOptions []oxia.DeleteRangeOption

	for _, test := range []struct {
		name               string
		args               string
		expectedParameters []any
	}{
		{"range", "--key-min a --key-max c", []any{"a", "c", emptyOptions}},
		{"short", "-s a -e c", []any{"a", "c", emptyOptions}},
		{"partition-key", "-s a -e c -p xyz", []any{"a", "c", []oxia.DeleteRangeOption{oxia.PartitionKey("xyz")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			common.MockedClient = common.NewMockClient()

			common.MockedClient.On("DeleteRange", test.expectedParameters...).Return(nil)
			_, err := runCmd(Cmd, test.args, "")
			assert.NoError(t, err)

			common.MockedClient.AssertExpectations(t)
		})
	}
}
