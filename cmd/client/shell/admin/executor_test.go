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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	admincommons "github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/oxia"
)

var errTest = errors.New("test failure")

func TestExecutorExecuteRejectsUnknownCommands(t *testing.T) {
	for _, test := range []struct {
		name  string
		args  []string
		error string
	}{
		{"empty", nil, "usage: admin namespace|dataserver|shard COMMAND [ARGS...]"},
		{"unknown-resource", []string{"cluster"}, `unknown admin command "cluster"`},
		{"namespace-empty", []string{"namespace"}, "usage: admin namespace list|get|create|patch|delete"},
		{"namespace-unknown", []string{"namespaces", "rename"}, `unknown admin namespace command "rename"`},
		{"dataserver-empty", []string{"dataserver"}, "usage: admin dataserver list|get|create|patch|delete|drained"},
		{"dataserver-unknown", []string{"data-servers", "rename"}, `unknown admin dataserver command "rename"`},
		{"shard-empty", []string{"shard"}, "usage: admin shard split --shard ID [--namespace NAME] [--split-point HASH]"},
		{"shard-unknown", []string{"shards", "merge"}, `unknown admin shard command "merge"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			adminClient := admincommons.NewMockAdminClient()
			executor, out := newTestExecutor(adminClient)

			assert.EqualError(t, executor.Execute(test.args), test.error)
			assert.Empty(t, out.String())
			adminClient.AssertExpectations(t)
		})
	}
}

func TestExecutorExecuteRejectsInvalidOutputCommands(t *testing.T) {
	for _, test := range []struct {
		name  string
		args  []string
		error string
	}{
		{"list-unknown-flag", []string{"namespace", "list", "--unknown"}, "usage: admin namespace list [--output FORMAT]"},
		{"list-extra-arg", []string{"namespace", "list", "ns"}, "usage: admin namespace list [--output FORMAT]"},
		{"list-output", []string{"dataserver", "list", "-o", "name"},
			`unsupported output format "name", expected one of: json, yaml, table`},
		{"get-unknown-flag", []string{"namespace", "get", "--unknown", "ns"},
			"usage: admin namespace get [--output FORMAT] NAMESPACE"},
		{"get-no-name", []string{"namespace", "get"}, "usage: admin namespace get [--output FORMAT] NAMESPACE"},
		{"get-output", []string{"dataserver", "get", "-o", "xml", "server"},
			`unsupported output format "xml", expected one of: json, yaml, table`},
		{"get-blank-name", []string{"dataserver", "get", " "}, "usage: admin dataserver get [--output FORMAT] NAME"},
		{"namespace-delete-no-name", []string{"namespace", "delete"},
			"usage: admin namespace delete [--output FORMAT] NAMESPACE"},
		{"dataserver-delete-no-name", []string{"dataserver", "delete"},
			"usage: admin dataserver delete [--output FORMAT] NAME"},
	} {
		t.Run(test.name, func(t *testing.T) {
			adminClient := admincommons.NewMockAdminClient()
			executor, out := newTestExecutor(adminClient)

			assert.EqualError(t, executor.Execute(test.args), test.error)
			assert.Empty(t, out.String())
			adminClient.AssertExpectations(t)
		})
	}
}

func TestExecutorExecuteReturnsClientFactoryError(t *testing.T) {
	for _, args := range [][]string{
		{"namespace", "list"},
		{"namespace", "get", "ns"},
		{"namespace", "create", "--initial-shards", "1", "--replication-factor", "1", "ns"},
		{"namespace", "patch", "--replication-factor", "1", "ns"},
		{"namespace", "delete", "ns"},
		{"dataserver", "list"},
		{"dataserver", "get", "server"},
		{"dataserver", "create", "--public", "p:1", "--internal", "i:1", "server"},
		{"dataserver", "patch", "--public", "p:1", "server"},
		{"dataserver", "delete", "server"},
		{"dataserver", "drained", "server"},
		{"shard", "split", "--shard", "1"},
	} {
		executor := Executor{
			Ctx:    context.Background(),
			Client: func() (oxia.AdminClient, error) { return nil, errTest },
			Out:    new(bytes.Buffer),
		}

		assert.ErrorIs(t, executor.Execute(args), errTest, "args: %v", args)
	}
}

func newTestExecutor(adminClient oxia.AdminClient) (Executor, *bytes.Buffer) {
	out := new(bytes.Buffer)
	return Executor{
		Ctx:    context.Background(),
		Client: func() (oxia.AdminClient, error) { return adminClient, nil },
		Out:    out,
	}, out
}
