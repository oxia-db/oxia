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

package option

import (
	"github.com/spf13/cobra"

	cmdparse "github.com/oxia-db/oxia/cmd/common/parse"
)

const (
	InitialShardsFlagName     = "initial-shards"
	ReplicationFactorFlagName = "replication-factor"
	NotificationsFlagName     = "notifications"
	KeySortingFlagName        = "key-sorting"
	AntiAffinityFlagName      = "anti-affinity"
)

type NamespaceFields struct {
	InitialShardCount uint32
	ReplicationFactor uint32
	Notifications     bool
	KeySorting        string
	AntiAffinities    []string
}

func (f *NamespaceFields) AddFlags(cmd *cobra.Command) {
	f.Reset()
	cmd.Flags().Uint32Var(&f.InitialShardCount, InitialShardsFlagName, 0, "Initial shard count for the namespace")
	cmd.Flags().Uint32Var(&f.ReplicationFactor, ReplicationFactorFlagName, 0, "Replication factor for the namespace")
	cmd.Flags().BoolVar(&f.Notifications, NotificationsFlagName, true, "Whether notifications are enabled")
	cmd.Flags().StringVar(&f.KeySorting, KeySortingFlagName, f.KeySorting, `Key sorting. allowed: "hierarchical", "natural"`)
	addAntiAffinityFlag(cmd, &f.AntiAffinities)
	_ = cmd.RegisterFlagCompletionFunc(KeySortingFlagName, keySortingCompletion)
}

func (f *NamespaceFields) AddPatchFlags(cmd *cobra.Command) {
	f.Reset()
	cmd.Flags().Uint32Var(&f.ReplicationFactor, ReplicationFactorFlagName, 0, "Replication factor for the namespace")
	cmd.Flags().BoolVar(&f.Notifications, NotificationsFlagName, true, "Whether notifications are enabled")
	addAntiAffinityFlag(cmd, &f.AntiAffinities)
}

func (f *NamespaceFields) Reset() {
	f.InitialShardCount = 0
	f.ReplicationFactor = 0
	f.Notifications = true
	f.KeySorting = "hierarchical"
	f.AntiAffinities = nil
}

func keySortingCompletion(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{
		"hierarchical\tUse file-system like hierarchical sorting based on `/`",
		"natural\tUse natural, byte-wise sorting",
	}, cobra.ShellCompDirectiveDefault
}

func addAntiAffinityFlag(cmd *cobra.Command, target *[]string) {
	cmd.Flags().StringArrayVar(
		target,
		AntiAffinityFlagName,
		nil,
		`Anti-affinity rule in labels=mode form. Example: "--anti-affinity=zone=strict" or "--anti-affinity=zone,rack=strict". Use without a value to clear.`,
	)
	cmd.Flags().Lookup(AntiAffinityFlagName).NoOptDefVal = cmdparse.AntiAffinityClearValue
}
