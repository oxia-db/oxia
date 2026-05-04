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

package policy

import (
	getcmd "github.com/oxia-db/oxia/cmd/admin/policy/get"
	patchcmd "github.com/oxia-db/oxia/cmd/admin/policy/patch"

	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "policy",
	Short: "Oxia admin operations on cluster policy defaults",
	Long:  `Oxia admin operations on cluster policy defaults`,
}

func init() {
	Cmd.AddCommand(getcmd.Cmd)
	Cmd.AddCommand(patchcmd.Cmd)
}
