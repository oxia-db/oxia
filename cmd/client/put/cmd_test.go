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

package put

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/client/common"
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

func TestPut_exec(t *testing.T) {
	var emptyOptions []oxia.PutOption
	for _, test := range []struct {
		name               string
		args               string
		stdin              string
		expectedParameters []any
	}{
		{"entry", "x y", "", []any{"x", []byte("y"), emptyOptions}},
		{"entry-expected-version", "x y -e 5", "", []any{"x", []byte("y"), []oxia.PutOption{oxia.ExpectedVersionId(5)}}},
		{"stdin", "x -c -e 5", "my-value", []any{"x", []byte("my-value"), []oxia.PutOption{oxia.ExpectedVersionId(5)}}},
		{"partition-key", "x y -p abc", "", []any{"x", []byte("y"), []oxia.PutOption{oxia.PartitionKey("abc")}}},
		{"sequence-keys", "x y -p abc -d 1,2,3", "", []any{"x", []byte("y"),
			[]oxia.PutOption{oxia.PartitionKey("abc"), oxia.SequenceKeysDeltas(1, 2, 3)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			common.MockedClient = common.NewMockClient()

			common.MockedClient.On("Put", test.expectedParameters...).Return(test.expectedParameters[0], oxia.Version{}, nil)
			_, _ = runCmd(Cmd, test.args, test.stdin)

			common.MockedClient.AssertExpectations(t)
		})
	}
}
