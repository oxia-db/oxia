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

package admin

import (
	"fmt"

	"github.com/oxia-db/oxia/cmd/admin/commons"

	"github.com/oxia-db/oxia/cmd/admin/listnamespaces"

	oxiacommon "github.com/oxia-db/oxia/common/constant"

	"github.com/spf13/cobra"
)

var (
	Cmd = &cobra.Command{
		Use:   "admin",
		Short: "Oxia admin operations to manage the cluster",
		Long:  `Oxia admin operations to manage the cluster`,
	}
)

func init() {
	defaultAdminClientAddress := fmt.Sprintf("localhost:%d", oxiacommon.DefaultAdminPort)
	Cmd.PersistentFlags().StringVar(&commons.AdminConfig.AdminAddress, "admin-address", defaultAdminClientAddress, "Admin client address")

	Cmd.AddCommand(listnamespaces.Cmd)
}
