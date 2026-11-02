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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	admincommons "github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

func TestExecutorDataServerReturnsAdminClientError(t *testing.T) {
	adminClient := admincommons.NewMockAdminClient()
	adminClient.On("ListDataServers").Return([]*proto.DataServerView(nil), errTest)
	adminClient.On("GetDataServer", "server").Return((*proto.DataServerView)(nil), errTest)
	adminClient.On("CreateDataServer", mock.Anything).Return((*proto.DataServer)(nil), errTest)
	adminClient.On("PatchDataServer", mock.Anything).Return((*proto.DataServer)(nil), errTest)
	adminClient.On("DeleteDataServer", "server").Return((*proto.DataServer)(nil), errTest)
	adminClient.On("DrainedDataServer", "server").Return(errTest)
	executor, out := newTestExecutor(adminClient)

	for _, args := range [][]string{
		{"dataserver", "list"},
		{"dataserver", "get", "server"},
		{"dataserver", "create", "--public", "p:1", "--internal", "i:1", "server"},
		{"dataserver", "patch", "--internal", "i:2", "server"},
		{"dataserver", "del", "server"},
		{"dataserver", "drained", "server"},
	} {
		assert.ErrorIs(t, executor.Execute(args), errTest, "args: %v", args)
	}
	assert.Empty(t, out.String())
	adminClient.AssertExpectations(t)
}

func TestExecutorDataServerCreateRejectsInvalidArgs(t *testing.T) {
	const usage = "usage: admin dataserver create [--output FORMAT] --public ADDRESS --internal ADDRESS " +
		"[--label KEY=VALUE] NAME"
	for _, test := range []struct {
		name  string
		args  []string
		error string
	}{
		{"unknown-flag", []string{"--unknown", "server"}, usage},
		{"no-name", []string{"--public", "p:1", "--internal", "i:1"}, usage},
		{"blank-name", []string{"--public", "p:1", "--internal", "i:1", " "}, "data server name must not be empty"},
		{"no-public", []string{"--internal", "i:1", "server"}, "data server public address must not be empty"},
		{"no-internal", []string{"--public", "p:1", "server"}, "data server internal address must not be empty"},
		{"invalid-label", []string{"--public", "p:1", "--internal", "i:1", "--label", "zone", "server"},
			`invalid entry "zone", expected key=value`},
		{"invalid-output", []string{"--public", "p:1", "--internal", "i:1", "-o", "name", "server"},
			`unsupported output format "name", expected one of: json, yaml, table`},
	} {
		t.Run(test.name, func(t *testing.T) {
			adminClient := admincommons.NewMockAdminClient()
			executor, out := newTestExecutor(adminClient)

			err := executor.Execute(append([]string{"dataserver", "create"}, test.args...))

			assert.EqualError(t, err, test.error)
			assert.Empty(t, out.String())
			adminClient.AssertExpectations(t)
		})
	}
}

func TestExecutorDataServerPatchSetsInternalAndLabels(t *testing.T) {
	adminClient := admincommons.NewMockAdminClient()
	adminClient.On("PatchDataServer", mock.MatchedBy(func(dataServer *proto.DataServer) bool {
		return dataServer.GetNameOrDefault() == "server" &&
			dataServer.GetIdentity().GetPublic() == "" &&
			dataServer.GetIdentity().GetInternal() == "i:2" &&
			dataServer.GetMetadata().GetLabels()["zone"] == "b"
	})).Return(&proto.DataServer{Identity: &proto.DataServerIdentity{Internal: "i:2"}}, nil)
	executor, out := newTestExecutor(adminClient)

	require.NoError(t, executor.Execute([]string{
		"dataserver", "patch", "--internal", "i:2", "--label", "zone=b", "-o", "json", "server",
	}))

	assert.Contains(t, out.String(), "i:2")
	adminClient.AssertExpectations(t)
}

func TestExecutorDataServerPatchRejectsInvalidArgs(t *testing.T) {
	const usage = "usage: admin dataserver patch [--output FORMAT] [--public ADDRESS] [--internal ADDRESS] " +
		"[--label KEY=VALUE] NAME"
	for _, test := range []struct {
		name  string
		args  []string
		error string
	}{
		{"unknown-flag", []string{"--unknown", "server"}, usage},
		{"no-name", []string{"--public", "p:1"}, usage},
		{"blank-name", []string{"--public", "p:1", " "}, "data server name must not be empty"},
		{"no-fields", []string{"server"}, "must specify at least one field to patch"},
		{"blank-public", []string{"--public", " ", "server"}, "data server public address must not be empty"},
		{"blank-internal", []string{"--internal", "", "server"}, "data server internal address must not be empty"},
		{"invalid-label", []string{"--label", "=b", "server"}, `invalid entry "=b", expected key=value`},
		{"invalid-output", []string{"--public", "p:1", "-o", "name", "server"},
			`unsupported output format "name", expected one of: json, yaml, table`},
	} {
		t.Run(test.name, func(t *testing.T) {
			adminClient := admincommons.NewMockAdminClient()
			executor, out := newTestExecutor(adminClient)

			err := executor.Execute(append([]string{"dataserver", "patch"}, test.args...))

			assert.EqualError(t, err, test.error)
			assert.Empty(t, out.String())
			adminClient.AssertExpectations(t)
		})
	}
}

func TestExecutorDataServerDrainedSendsTheName(t *testing.T) {
	adminClient := admincommons.NewMockAdminClient()
	adminClient.On("DrainedDataServer", "server").Return(nil)
	executor, out := newTestExecutor(adminClient)

	require.NoError(t, executor.Execute([]string{"dataserver", "drained", " server "}))

	assert.Empty(t, out.String())
	adminClient.AssertExpectations(t)
}

func TestExecutorDataServerDrainedRejectsInvalidArgs(t *testing.T) {
	for _, test := range []struct {
		name  string
		args  []string
		error string
	}{
		{"no-name", nil, "usage: admin dataserver drained NAME"},
		{"two-names", []string{"a", "b"}, "usage: admin dataserver drained NAME"},
		{"blank-name", []string{" "}, "data server name must not be empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			adminClient := admincommons.NewMockAdminClient()
			executor, out := newTestExecutor(adminClient)

			err := executor.Execute(append([]string{"dataserver", "drained"}, test.args...))

			assert.EqualError(t, err, test.error)
			assert.Empty(t, out.String())
			adminClient.AssertExpectations(t)
		})
	}
}
