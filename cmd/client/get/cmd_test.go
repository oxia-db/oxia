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

package get

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"

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

func Test_exec(t *testing.T) {
	var emptyOptions []oxia.GetOption
	for _, test := range []struct {
		name               string
		args               string
		expectedParameters []any
		out                string
	}{
		{"key", "x", []any{"x", emptyOptions}, "value-x"},
		{"other-key", "y", []any{"y", emptyOptions}, "value-y"},
		{"partition-key", "y -p xyz", []any{"y", []oxia.GetOption{oxia.PartitionKey("xyz")}}, "value-y"},
		{"equal", "y -t equal", []any{"y", emptyOptions}, "value-y"},
		{"floor", "y -t floor", []any{"y", []oxia.GetOption{oxia.ComparisonFloor()}}, "value-y"},
		{"ceiling", "y -t ceiling", []any{"y", []oxia.GetOption{oxia.ComparisonCeiling()}}, "value-y"},
		{"lower", "y -t lower", []any{"y", []oxia.GetOption{oxia.ComparisonLower()}}, "value-y"},
		{"higher", "y -t higher", []any{"y", []oxia.GetOption{oxia.ComparisonHigher()}}, "value-y"},
	} {
		t.Run(test.name, func(t *testing.T) {
			common.MockedClient = common.NewMockClient()

			common.MockedClient.On("Get", test.expectedParameters...).
				Return(test.expectedParameters[0], []byte(test.out), oxia.Version{}, nil)
			out, err := runCmd(Cmd, test.args, "")
			assert.NoError(t, err)
			assert.Equal(t, test.out, out)

			common.MockedClient.AssertExpectations(t)
		})
	}
}
