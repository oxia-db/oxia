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

package del

import (
	"bytes"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

func runCmd(cmd *cobra.Command, args string) (string, error) {
	actual := new(bytes.Buffer)
	cmd.SetOut(actual)
	cmd.SetErr(actual)
	cmd.SetArgs(strings.Split(args, " "))
	err := cmd.Execute()
	Config.Reset()
	return strings.TrimSpace(actual.String()), err
}

func TestDelete_exec(t *testing.T) {
	var emptyOptions []oxia.DeleteOption
	common.MockedClient = common.NewMockClient()

	common.MockedClient.On("Delete", "my-key", emptyOptions).Return(nil)
	out, err := runCmd(Cmd, "my-key")
	assert.NoError(t, err)
	assert.Empty(t, out)

	common.MockedClient.On("Delete", "my-key", []oxia.DeleteOption{oxia.ExpectedVersionId(5)}).Return(nil)
	out, err = runCmd(Cmd, "my-key -e 5")
	assert.NoError(t, err)
	assert.Empty(t, out)

	common.MockedClient.On("Delete", "xyz", emptyOptions).Return(errors.New("failed to delete"))
	out, err = runCmd(Cmd, "xyz")
	assert.Error(t, err)
	assert.Equal(t, "Error: failed to delete", out)

	common.MockedClient.On("Delete", "my-key", []oxia.DeleteOption{oxia.PartitionKey("xyz")}).Return(nil)
	out, err = runCmd(Cmd, "my-key -p xyz")
	assert.NoError(t, err)
	assert.Empty(t, out)

	common.MockedClient.On("Delete", "my-key",
		[]oxia.DeleteOption{oxia.ExpectedVersionId(4), oxia.PartitionKey("xyz")}).Return(nil)
	out, err = runCmd(Cmd, "my-key -e 4 -p xyz")
	assert.NoError(t, err)
	assert.Empty(t, out)

	common.MockedClient.AssertExpectations(t)
}
