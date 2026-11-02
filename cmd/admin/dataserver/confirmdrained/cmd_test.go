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

package confirmdrained

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/cmd/common/clientauth"
)

func runCmd(args ...string) (string, error) {
	actual := new(bytes.Buffer)
	root := &cobra.Command{Use: "admin"}
	root.AddCommand(&cobra.Command{
		Use:          Cmd.Use,
		Args:         Cmd.Args,
		RunE:         Cmd.RunE,
		SilenceUsage: Cmd.SilenceUsage,
	})
	root.SetOut(actual)
	root.SetErr(actual)
	root.SetArgs(append([]string{"confirm-drained"}, args...))
	err := root.Execute()
	return strings.TrimSpace(actual.String()), err
}

func TestExecConfirmsTheDrain(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })
	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("ConfirmDrainedDataServer", "server-1").Return(nil)

	out, err := runCmd(" server-1 ")

	require.NoError(t, err)
	assert.Empty(t, out)
	commons.MockedAdminClient.AssertExpectations(t)
}

func TestExecRejectsEmptyName(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })

	_, err := runCmd("   ")

	require.EqualError(t, err, "data server name must not be empty")
	commons.MockedAdminClient.AssertExpectations(t)
}

func TestExecReturnsAdminClientError(t *testing.T) {
	commons.MockedAdminClient = nil
	commons.AdminConfig.Auth = clientauth.Config{Token: "token", TokenFile: "token-file"}
	t.Cleanup(func() { commons.AdminConfig.Auth = clientauth.Config{} })

	_, err := runCmd("server-1")

	require.ErrorIs(t, err, clientauth.ErrAuthTokenConflict)
}

func TestExecReturnsConfirmError(t *testing.T) {
	commons.MockedAdminClient = commons.NewMockAdminClient()
	t.Cleanup(func() { commons.MockedAdminClient = nil })
	commons.MockedAdminClient.On("Close").Return(nil)
	commons.MockedAdminClient.On("ConfirmDrainedDataServer", "server-1").Return(errors.New("not drained"))

	out, err := runCmd("server-1")

	require.EqualError(t, err, "not drained")
	assert.Contains(t, out, "not drained")
	commons.MockedAdminClient.AssertExpectations(t)
}
