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

package proto

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/oxia-db/oxia/common/validation"
)

const (
	defaultLoadBalancerScheduleInterval = 30 * time.Second
	defaultLoadBalancerQuarantineTime   = 5 * time.Minute
	defaultLoadBalancerScheduleString   = "30s"
	defaultLoadBalancerQuarantineString = "5m"

	defaultAutoSplitMaxShardSizeMB       uint32 = 1024
	defaultAutoSplitMaxThroughputOps     uint32 = 10000
	defaultAutoSplitStabilizationPeriod         = 1 * time.Minute
	defaultAutoSplitCooldownPeriod              = 5 * time.Minute
	defaultAutoSplitStabilizationString         = "1m"
	defaultAutoSplitCooldownString              = "5m"
	defaultMaxShardsPerNamespace         uint32 = 64

	AntiAffinityModeUnknown = ""
	AntiAffinityModeStrict  = "strict"
	AntiAffinityModeRelaxed = "relaxed"

	ShardStatusUnknown     = ShardStatus_ShardStatusUnknown
	ShardStatusSteadyState = ShardStatus_SteadyState
	ShardStatusElection    = ShardStatus_Election
	ShardStatusDeleting    = ShardStatus_Deleting

	SplitPhaseBootstrap = SplitPhase_Bootstrap
	SplitPhaseCatchUp   = SplitPhase_CatchUp
	SplitPhaseCutover   = SplitPhase_Cutover
)

func (ds *DataServerIdentity) GetNameOrDefault() string {
	if ds == nil {
		return ""
	}
	if ds.Name != nil {
		return ds.GetName()
	}
	return ds.GetInternal()
}

func (ds *DataServer) GetNameOrDefault() string {
	if ds == nil {
		return ""
	}
	return ds.GetIdentity().GetNameOrDefault()
}

func NewClusterStatus() *ClusterStatus {
	return &ClusterStatus{
		Namespaces: map[string]*NamespaceStatus{},
	}
}

func ParseShardStatus(value ShardStatus) ShardStatus {
	switch value {
	case ShardStatusSteadyState:
		return ShardStatusSteadyState
	case ShardStatusElection:
		return ShardStatusElection
	case ShardStatusDeleting:
		return ShardStatusDeleting
	default:
		return ShardStatusUnknown
	}
}

func (sm *ShardMetadata) GetStatusOrDefault() ShardStatus {
	if sm == nil {
		return ShardStatusUnknown
	}
	return ParseShardStatus(sm.GetStatus())
}

func ParseSplitPhase(value SplitPhase) SplitPhase {
	switch value {
	case SplitPhaseCatchUp:
		return SplitPhaseCatchUp
	case SplitPhaseCutover:
		return SplitPhaseCutover
	default:
		return SplitPhaseBootstrap
	}
}

func (sm *SplitMetadata) GetPhaseOrDefault() SplitPhase {
	if sm == nil {
		return SplitPhaseBootstrap
	}
	return ParseSplitPhase(sm.GetPhase())
}

func (ns *Namespace) NotificationsEnabledOrDefault() bool {
	if ns == nil || ns.NotificationsEnabled == nil {
		return true
	}
	return ns.GetNotificationsEnabled()
}

func (ns *Namespace) GetKeySortingType() (KeySortingType, error) {
	if ns == nil {
		return KeySortingType_UNKNOWN, nil
	}
	return ParseKeySortingType(ns.GetKeySorting())
}

func (ns *Namespace) SetKeySortingType(value KeySortingType) {
	if ns == nil {
		return
	}
	ns.KeySorting = formatKeySortingType(value)
}

func ParseKeySortingType(value string) (KeySortingType, error) {
	switch strings.ToLower(value) {
	case "", "unknown":
		return KeySortingType_UNKNOWN, nil
	case "natural":
		return KeySortingType_NATURAL, nil
	case "hierarchical":
		return KeySortingType_HIERARCHICAL, nil
	default:
		return KeySortingType_UNKNOWN, errors.New(`must be one of "natural" or "hierarchical"`)
	}
}

func ParseAntiAffinityMode(value string) string {
	switch strings.ToLower(value) {
	case AntiAffinityModeStrict:
		return AntiAffinityModeStrict
	case AntiAffinityModeRelaxed:
		return AntiAffinityModeRelaxed
	default:
		return AntiAffinityModeUnknown
	}
}

func (a *AntiAffinity) GetModeOrDefault() string {
	if a == nil {
		return AntiAffinityModeUnknown
	}
	return ParseAntiAffinityMode(a.GetMode())
}

func (a *AntiAffinity) SetModeOrDefault(value string) {
	if a == nil {
		return
	}
	a.Mode = ParseAntiAffinityMode(value)
}

func (cc *ClusterConfiguration) GetDataServer(id string) (*DataServer, bool) {
	if cc == nil {
		return nil, false
	}

	for _, server := range cc.GetServers() {
		if server.GetNameOrDefault() != id {
			continue
		}

		identity := server
		if server.GetName() == "" {
			name := server.GetNameOrDefault()
			identity = &DataServerIdentity{
				Name:     &name,
				Public:   server.GetPublic(),
				Internal: server.GetInternal(),
			}
		}

		dataServer := &DataServer{
			Identity: identity,
			Metadata: &DataServerMetadata{},
		}
		if metadata, found := cc.GetServerMetadata()[id]; found {
			dataServer.Metadata = metadata
		}
		return dataServer, true
	}

	return nil, false
}

func (cc *ClusterConfiguration) Validate() error {
	if cc == nil {
		return errors.New("cluster configuration: must not be nil")
	}

	for _, ns := range cc.GetNamespaces() {
		if err := validation.ValidateNamespace(ns.GetName()); err != nil {
			return fmt.Errorf("cluster configuration: %w", err)
		}

		if ns.GetReplicationFactor() < 1 {
			return fmt.Errorf("cluster configuration: namespace %q has invalid replicationFactor=%d, must be >= 1",
				ns.GetName(), ns.GetReplicationFactor())
		}

		if ns.GetInitialShardCount() < 1 {
			return fmt.Errorf("cluster configuration: namespace %q has invalid initialShardCount=%d, must be >= 1",
				ns.GetName(), ns.GetInitialShardCount())
		}

		if _, err := ns.GetKeySortingType(); err != nil {
			return fmt.Errorf("cluster configuration: namespace %q has invalid keySorting: %w", ns.GetName(), err)
		}

		for idx, antiAffinity := range ns.GetAntiAffinities() {
			if err := antiAffinity.Validate(); err != nil {
				return fmt.Errorf("cluster configuration: namespace %q has invalid antiAffinities[%d]: %w",
					ns.GetName(), idx, err)
			}
		}

		if ns.GetReplicationFactor() > uint32(len(cc.GetServers())) {
			return fmt.Errorf("cluster configuration: namespace %q has replicationFactor=%d but only %d servers are configured",
				ns.GetName(), ns.GetReplicationFactor(), len(cc.GetServers()))
		}
	}

	if loadBalancer := cc.GetLoadBalancer(); loadBalancer != nil {
		if _, err := loadBalancer.GetScheduleIntervalDuration(); err != nil {
			return fmt.Errorf("cluster configuration: invalid loadBalancer.scheduleInterval: %w", err)
		}
		if _, err := loadBalancer.GetQuarantineTimeDuration(); err != nil {
			return fmt.Errorf("cluster configuration: invalid loadBalancer.quarantineTime: %w", err)
		}
	}

	return nil
}

func (a *AntiAffinity) Validate() error {
	if len(a.GetLabels()) == 0 {
		return errors.New("labels must not be empty")
	}
	for _, label := range a.GetLabels() {
		if strings.TrimSpace(label) == "" {
			return errors.New("labels must not contain empty values")
		}
	}
	if a.GetModeOrDefault() == AntiAffinityModeUnknown {
		return errors.New(`mode must be one of "strict" or "relaxed"`)
	}
	return nil
}

func (lb *LoadBalancer) GetScheduleIntervalDuration() (time.Duration, error) {
	if lb == nil || lb.GetScheduleInterval() == "" {
		return 0, nil
	}
	return time.ParseDuration(lb.GetScheduleInterval())
}

func (lb *LoadBalancer) SetScheduleIntervalDuration(value time.Duration) {
	if lb == nil {
		return
	}
	lb.ScheduleInterval = value.String()
}

func (lb *LoadBalancer) GetQuarantineTimeDuration() (time.Duration, error) {
	if lb == nil || lb.GetQuarantineTime() == "" {
		return 0, nil
	}
	return time.ParseDuration(lb.GetQuarantineTime())
}

func (lb *LoadBalancer) SetQuarantineTimeDuration(value time.Duration) {
	if lb == nil {
		return
	}
	lb.QuarantineTime = value.String()
}

func (lb *LoadBalancer) GetScheduleIntervalDurationOrDefault() time.Duration {
	if lb == nil {
		return defaultLoadBalancerScheduleInterval
	}
	duration, err := lb.GetScheduleIntervalDuration()
	if err != nil || duration == 0 {
		return defaultLoadBalancerScheduleInterval
	}
	return duration
}

func (lb *LoadBalancer) GetQuarantineTimeDurationOrDefault() time.Duration {
	if lb == nil {
		return defaultLoadBalancerQuarantineTime
	}
	duration, err := lb.GetQuarantineTimeDuration()
	if err != nil || duration == 0 {
		return defaultLoadBalancerQuarantineTime
	}
	return duration
}

func (cc *ClusterConfiguration) GetLoadBalancerWithDefaults() *LoadBalancer {
	loadBalancer := &LoadBalancer{
		ScheduleInterval: defaultLoadBalancerScheduleString,
		QuarantineTime:   defaultLoadBalancerQuarantineString,
	}

	if cc == nil || cc.GetLoadBalancer() == nil {
		return loadBalancer
	}

	if value := cc.GetLoadBalancer().GetScheduleInterval(); value != "" {
		loadBalancer.ScheduleInterval = value
	}
	if value := cc.GetLoadBalancer().GetQuarantineTime(); value != "" {
		loadBalancer.QuarantineTime = value
	}

	return loadBalancer
}

// AutoSplitConfig helpers — follow the same pattern as LoadBalancer duration fields.

func (c *AutoSplitConfig) GetStabilizationPeriodDuration() (time.Duration, error) {
	if c == nil || c.GetStabilizationPeriod() == "" {
		return 0, nil
	}
	return time.ParseDuration(c.GetStabilizationPeriod())
}

func (c *AutoSplitConfig) GetStabilizationPeriodDurationOrDefault() time.Duration {
	if c == nil {
		return defaultAutoSplitStabilizationPeriod
	}
	duration, err := c.GetStabilizationPeriodDuration()
	if err != nil || duration == 0 {
		return defaultAutoSplitStabilizationPeriod
	}
	return duration
}

func (c *AutoSplitConfig) GetCooldownPeriodDuration() (time.Duration, error) {
	if c == nil || c.GetCooldownPeriod() == "" {
		return 0, nil
	}
	return time.ParseDuration(c.GetCooldownPeriod())
}

func (c *AutoSplitConfig) GetCooldownPeriodDurationOrDefault() time.Duration {
	if c == nil {
		return defaultAutoSplitCooldownPeriod
	}
	duration, err := c.GetCooldownPeriodDuration()
	if err != nil || duration == 0 {
		return defaultAutoSplitCooldownPeriod
	}
	return duration
}

func (c *AutoSplitConfig) GetMaxShardSizeMBOrDefault() uint32 {
	if c == nil || c.GetMaxShardSizeMb() == 0 {
		return defaultAutoSplitMaxShardSizeMB
	}
	return c.GetMaxShardSizeMb()
}

func (c *AutoSplitConfig) GetMaxThroughputOpsOrDefault() uint32 {
	if c == nil || c.GetMaxThroughputOps() == 0 {
		return defaultAutoSplitMaxThroughputOps
	}
	return c.GetMaxThroughputOps()
}

func (sm *ShardManagement) GetMaxShardsPerNamespaceOrDefault() uint32 {
	if sm == nil || sm.GetMaxShardsPerNamespace() == 0 {
		return defaultMaxShardsPerNamespace
	}
	return sm.GetMaxShardsPerNamespace()
}

func (cc *ClusterConfiguration) GetShardManagementWithDefaults() *ShardManagement {
	sm := &ShardManagement{
		MaxShardsPerNamespace: defaultMaxShardsPerNamespace,
		AutoSplit: &AutoSplitConfig{
			MaxShardSizeMb:      defaultAutoSplitMaxShardSizeMB,
			MaxThroughputOps:    defaultAutoSplitMaxThroughputOps,
			StabilizationPeriod: defaultAutoSplitStabilizationString,
			CooldownPeriod:      defaultAutoSplitCooldownString,
		},
	}

	if cc == nil || cc.GetShardManagement() == nil {
		return sm
	}

	src := cc.GetShardManagement()
	if v := src.GetMaxShardsPerNamespace(); v != 0 {
		sm.MaxShardsPerNamespace = v
	}

	if src.GetAutoSplit() != nil {
		as := src.GetAutoSplit()
		sm.AutoSplit.Enabled = as.GetEnabled()
		if v := as.GetMaxShardSizeMb(); v != 0 {
			sm.AutoSplit.MaxShardSizeMb = v
		}
		if v := as.GetMaxThroughputOps(); v != 0 {
			sm.AutoSplit.MaxThroughputOps = v
		}
		if v := as.GetStabilizationPeriod(); v != "" {
			sm.AutoSplit.StabilizationPeriod = v
		}
		if v := as.GetCooldownPeriod(); v != "" {
			sm.AutoSplit.CooldownPeriod = v
		}
	}

	return sm
}

func formatKeySortingType(value KeySortingType) string {
	switch value {
	case KeySortingType_NATURAL:
		return "natural"
	case KeySortingType_HIERARCHICAL:
		return "hierarchical"
	default:
		return ""
	}
}
