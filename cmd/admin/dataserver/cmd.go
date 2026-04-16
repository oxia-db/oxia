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

package dataserver

import (
	"github.com/spf13/cobra"

	createcmd "github.com/oxia-db/oxia/cmd/admin/dataserver/create"
	getcmd "github.com/oxia-db/oxia/cmd/admin/dataserver/get"
	listcmd "github.com/oxia-db/oxia/cmd/admin/dataserver/list"
)

var (
	Cmd = &cobra.Command{
		Use:   "dataserver",
		Short: "Oxia admin operations on data servers",
		Long:  `Oxia admin operations on data servers`,
	}
)

func init() {
	Cmd.AddCommand(createcmd.Cmd)
	Cmd.AddCommand(getcmd.Cmd)
	Cmd.AddCommand(listcmd.Cmd)
}
