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

package pebble

import (
	"github.com/cockroachdb/pebble/tool"
	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/server/kv"
)

var (
	Cmd = &cobra.Command{
		Use:   "pebble",
		Short: "Pebble DB utils",
		Long:  `Tools for the Pebble DB`,
	}
)

func init() {
	t := tool.New(tool.DefaultComparer(kv.OxiaSlashSpanComparer))

	for _, cmd := range t.Commands {
		Cmd.AddCommand(cmd)
	}
}
