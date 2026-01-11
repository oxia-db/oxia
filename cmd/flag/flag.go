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

package flag

import (
	"fmt"

	"github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/common/constant"
)

func PublicAddr(cmd *cobra.Command, conf *string) {
	cmd.Flags().StringVarP(conf, "public-addr", "p", fmt.Sprintf("0.0.0.0:%d", constant.DefaultPublicPort), "Public service bind address")
}

func InternalAddr(cmd *cobra.Command, conf *string) {
	cmd.Flags().StringVarP(conf, "internal-addr", "i", fmt.Sprintf("0.0.0.0:%d", constant.DefaultInternalPort), "Internal service bind address")
}

func MetricsAddr(cmd *cobra.Command, conf *string) {
	cmd.Flags().StringVarP(conf, "metrics-addr", "m", fmt.Sprintf("0.0.0.0:%d", option.DefaultMetricsPort), "Metrics service bind address")
}

func AdminAddr(cmd *cobra.Command, conf *string) {
	cmd.Flags().StringVarP(conf, "admin-addr", "a", fmt.Sprintf("0.0.0.0:%d", constant.DefaultAdminPort), "Admin service bind address")
}
