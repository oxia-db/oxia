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

package client

import (
	"bytes"
	"strings"
	"testing"

	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/cmd/client/del"
	"github.com/oxia-db/oxia/cmd/client/get"
	"github.com/oxia-db/oxia/cmd/client/list"
	"github.com/oxia-db/oxia/cmd/client/put"
)

func TestClientCmd(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)

	stdin := bytes.NewBufferString("")
	stdout := bytes.NewBufferString("")
	stderr := bytes.NewBufferString("")
	Cmd.SetIn(stdin)
	Cmd.SetOut(stdout)
	Cmd.SetErr(stderr)

	for _, test := range []struct {
		name   string
		args   string
		stdin  string
		stdout string
		stderr string
		error  bool
	}{
		{"put", "put k-put a", "", "", "", false},
		{"put-expected", "put k-put c -e 0", "", "", "", false},
		{"put-unexpected-present", "put k-put c -e 0", "", "", "Error: unexpected version id", true},
		{"put-unexpected-not-present", "put k-put-unp c -e 9999", "", "", "Error: unexpected version id", true},
		{"put-stdin-not-present", "put k-put-stdin", "test-3a", "", "Error: no value provided for the record", true},
		{"put-stdin", "put k-put-stdin -c", "test-3b", "", "", false},
		{"get", "get k-put-stdin", "", "test-3b\n", "", false},
		{"get-not-exist", "get does-not-exist", "", "", "Error: key not found", true},
		{"list-none", "list --key-min XXX --key-max XXY", "", "", "", false},
		{"list-all", "list --key-min a --key-max z", "", "k-put\nk-put-stdin\n", "", false},
		{"list-all-short", "list -s a -e z", "", "k-put\nk-put-stdin\n", "", false},
		{"delete", "delete k-put-stdin", "", "", "", false},
		{"delete-not-exist", "delete does-not-exist", "", "", "Error: key not found", true},
		{"delete-unexpected-version", "delete k-put -e 9", "", "", "Error: unexpected version id", true},
		{"delete-expected-version", "delete k-put -e 1", "", "", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			put.Config.Reset()
			get.Config.Reset()
			list.Config.Reset()
			del.Config.Reset()

			stdin.WriteString(test.stdin)
			Cmd.SetArgs(append([]string{"-a", standaloneServer.ServiceAddr()}, strings.Split(test.args, " ")...))
			err := Cmd.Execute()
			if test.error {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if test.stdout != "" {
				assert.Equal(t, test.stdout, stdout.String())
			}
			assert.Equal(t, test.stderr, strings.TrimSpace(stderr.String()))

			stdin.Reset()
			stdout.Reset()
			stderr.Reset()
		})
	}
	_ = standaloneServer.Close()
}
