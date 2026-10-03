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


package standalone

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestStandalone_RejectsPositionalArgs(t *testing.T) {
	run := Cmd.Run
	t.Cleanup(func() { Cmd.Run = run })
	invoked := false
	Cmd.Run = func(*cobra.Command, []string) { invoked = true }

	// A bool flag never takes the next argument: "false" is a stray positional and the WAL sync stays on
	Cmd.SetArgs([]string{"--wal-sync-data", "false"})
	err := Cmd.Execute()

	assert.ErrorContains(t, err, `unknown command "false" for "standalone"`)
	assert.False(t, invoked)
}
