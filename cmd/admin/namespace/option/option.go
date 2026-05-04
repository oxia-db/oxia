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

	"github.com/oxia-db/oxia/common/proto"
)

const (
	InitialShardsFlagName     = "initial-shards"
	ReplicationFactorFlagName = "replication-factor"
	NotificationsFlagName     = "notifications"
	KeySortingFlagName        = "key-sorting"
)

type NamespaceFields struct {
	InitialShardCount uint32
	ReplicationFactor uint32
	Notifications     bool
	KeySorting        string
}

func (f *NamespaceFields) AddFlags(cmd *cobra.Command) {
	f.Reset()
	cmd.Flags().Uint32Var(&f.InitialShardCount, InitialShardsFlagName, 0, "Initial shard count for the namespace")
	cmd.Flags().Uint32Var(&f.ReplicationFactor, ReplicationFactorFlagName, 0, "Replication factor for the namespace")
	cmd.Flags().BoolVar(&f.Notifications, NotificationsFlagName, true, "Whether notifications are enabled")
	cmd.Flags().StringVar(&f.KeySorting, KeySortingFlagName, f.KeySorting, `Key sorting. allowed: "hierarchical", "natural"`)
	_ = cmd.RegisterFlagCompletionFunc(KeySortingFlagName, keySortingCompletion)
}

func (f *NamespaceFields) AddPolicyFlags(cmd *cobra.Command) {
	f.Reset()
	cmd.Flags().Uint32Var(&f.InitialShardCount, InitialShardsFlagName, 0, "Initial shard count")
	cmd.Flags().Uint32Var(&f.ReplicationFactor, ReplicationFactorFlagName, 0, "Replication factor")
	cmd.Flags().BoolVar(&f.Notifications, NotificationsFlagName, true, "Whether notifications are enabled")
	cmd.Flags().StringVar(&f.KeySorting, KeySortingFlagName, f.KeySorting, `Key sorting. allowed: "hierarchical", "natural"`)
	_ = cmd.RegisterFlagCompletionFunc(KeySortingFlagName, keySortingCompletion)
}

func (f *NamespaceFields) AddPatchFlags(cmd *cobra.Command) {
	f.Reset()
	cmd.Flags().Uint32Var(&f.ReplicationFactor, ReplicationFactorFlagName, 0, "Replication factor for the namespace")
	cmd.Flags().BoolVar(&f.Notifications, NotificationsFlagName, true, "Whether notifications are enabled")
}

func (f *NamespaceFields) ToPolicy() *proto.HierarchyPolicies {
	policy := &proto.HierarchyPolicies{}
	policy.SetInitialShardCount(f.InitialShardCount)
	policy.SetReplicationFactor(f.ReplicationFactor)
	policy.SetNotificationsEnabled(f.Notifications)
	policy.SetKeySorting(f.KeySorting)
	return policy
}

func (f *NamespaceFields) PatchPolicy(cmd *cobra.Command) (*proto.HierarchyPolicies, bool) {
	policy := &proto.HierarchyPolicies{}
	changed := false
	if cmd.Flags().Changed(InitialShardsFlagName) {
		policy.SetInitialShardCount(f.InitialShardCount)
		changed = true
	}
	if cmd.Flags().Changed(ReplicationFactorFlagName) {
		policy.SetReplicationFactor(f.ReplicationFactor)
		changed = true
	}
	if cmd.Flags().Changed(NotificationsFlagName) {
		policy.SetNotificationsEnabled(f.Notifications)
		changed = true
	}
	if cmd.Flags().Changed(KeySortingFlagName) {
		policy.SetKeySorting(f.KeySorting)
		changed = true
	}
	return policy, changed
}

func (f *NamespaceFields) Reset() {
	f.InitialShardCount = 0
	f.ReplicationFactor = 0
	f.Notifications = true
	f.KeySorting = "hierarchical"
}

func keySortingCompletion(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{
		"hierarchical\tUse file-system like hierarchical sorting based on `/`",
		"natural\tUse natural, byte-wise sorting",
	}, cobra.ShellCompDirectiveDefault
}
