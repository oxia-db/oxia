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
	defaultInitialShardCount            = uint32(1)
	defaultReplicationFactor            = uint32(1)
	defaultNotificationsEnabled         = true
	defaultKeySorting                   = "hierarchical"

	AntiAffinityModeUnknown = ""
	AntiAffinityModeStrict  = "strict"
	AntiAffinityModeRelaxed = "relaxed"

	ShardStatusUnknown     = "Unknown"
	ShardStatusSteadyState = "SteadyState"
	ShardStatusElection    = "Election"
	ShardStatusDeleting    = "Deleting"

	SplitPhaseBootstrap = "Bootstrap"
	SplitPhaseCatchUp   = "CatchUp"
	SplitPhaseCutover   = "Cutover"
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

func ParseShardStatus(value string) string {
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

func (sm *ShardMetadata) GetStatusOrDefault() string {
	if sm == nil {
		return ShardStatusUnknown
	}
	return ParseShardStatus(sm.GetStatus())
}

func ParseSplitPhase(value string) string {
	switch value {
	case SplitPhaseCatchUp:
		return SplitPhaseCatchUp
	case SplitPhaseCutover:
		return SplitPhaseCutover
	default:
		return SplitPhaseBootstrap
	}
}

func (sm *SplitMetadata) GetPhaseOrDefault() string {
	if sm == nil {
		return SplitPhaseBootstrap
	}
	return ParseSplitPhase(sm.GetPhase())
}

func (ns *Namespace) NotificationsEnabledOrDefault() bool {
	if ns == nil {
		return defaultNotificationsEnabled
	}
	if policy := ns.GetPolicy(); policy != nil && policy.NotificationsEnabled != nil {
		return policy.GetNotificationsEnabled()
	}
	if ns.NotificationsEnabled == nil {
		return defaultNotificationsEnabled
	}
	return ns.GetNotificationsEnabled()
}

func (ns *Namespace) GetKeySortingType() (KeySortingType, error) {
	if ns == nil {
		return KeySortingType_UNKNOWN, nil
	}
	if policy := ns.GetPolicy(); policy != nil && policy.KeySorting != nil {
		return ParseKeySortingType(policy.GetKeySorting())
	}
	return ParseKeySortingType(ns.GetKeySorting())
}

func (ns *Namespace) SetKeySortingType(value KeySortingType) {
	if ns == nil {
		return
	}
	keySorting := formatKeySortingType(value)
	if ns.Policy == nil {
		ns.Policy = &HierarchyPolicies{}
	}
	ns.Policy.SetKeySorting(keySorting)
	ns.KeySorting = keySorting
}

func NewDefaultHierarchyPolicies() *HierarchyPolicies {
	policy := &HierarchyPolicies{}
	policy.SetInitialShardCount(defaultInitialShardCount)
	policy.SetReplicationFactor(defaultReplicationFactor)
	policy.SetNotificationsEnabled(defaultNotificationsEnabled)
	policy.SetKeySorting(defaultKeySorting)
	return policy
}

func (p *HierarchyPolicies) SetInitialShardCount(value uint32) {
	if p == nil {
		return
	}
	p.InitialShardCount = &value
}

func (p *HierarchyPolicies) SetReplicationFactor(value uint32) {
	if p == nil {
		return
	}
	p.ReplicationFactor = &value
}

func (p *HierarchyPolicies) SetNotificationsEnabled(value bool) {
	if p == nil {
		return
	}
	p.NotificationsEnabled = &value
}

func (p *HierarchyPolicies) SetKeySorting(value string) {
	if p == nil {
		return
	}
	p.KeySorting = &value
}

func (p *HierarchyPolicies) GetKeySortingType() (KeySortingType, error) {
	if p == nil {
		return KeySortingType_UNKNOWN, nil
	}
	return ParseKeySortingType(p.GetKeySorting())
}

func CloneHierarchyPolicies(policy *HierarchyPolicies) *HierarchyPolicies {
	if policy == nil {
		return nil
	}

	cloned := &HierarchyPolicies{}
	if len(policy.GetAntiAffinities()) > 0 {
		cloned.AntiAffinities = make([]*AntiAffinity, 0, len(policy.GetAntiAffinities()))
		for _, antiAffinity := range policy.GetAntiAffinities() {
			if antiAffinity == nil {
				cloned.AntiAffinities = append(cloned.AntiAffinities, nil)
				continue
			}
			cloned.AntiAffinities = append(cloned.AntiAffinities, &AntiAffinity{
				Labels: append([]string(nil), antiAffinity.GetLabels()...),
				Mode:   antiAffinity.GetMode(),
			})
		}
	}
	if policy.InitialShardCount != nil {
		cloned.SetInitialShardCount(policy.GetInitialShardCount())
	}
	if policy.ReplicationFactor != nil {
		cloned.SetReplicationFactor(policy.GetReplicationFactor())
	}
	if policy.NotificationsEnabled != nil {
		cloned.SetNotificationsEnabled(policy.GetNotificationsEnabled())
	}
	if policy.KeySorting != nil {
		cloned.SetKeySorting(policy.GetKeySorting())
	}
	return cloned
}

func ResolveHierarchyPolicies(clusterPolicy *HierarchyPolicies, namespace *Namespace) *HierarchyPolicies {
	effective := NewDefaultHierarchyPolicies()
	applyHierarchyPolicy(effective, clusterPolicy)

	if namespace != nil {
		applyLegacyNamespacePolicy(effective, namespace)
		applyHierarchyPolicy(effective, namespace.GetPolicy())
	}

	return effective
}

func applyHierarchyPolicy(effective *HierarchyPolicies, policy *HierarchyPolicies) {
	if policy == nil {
		return
	}
	if len(policy.GetAntiAffinities()) > 0 {
		effective.AntiAffinities = CloneHierarchyPolicies(policy).GetAntiAffinities()
	}
	if policy.InitialShardCount != nil {
		effective.SetInitialShardCount(policy.GetInitialShardCount())
	}
	if policy.ReplicationFactor != nil {
		effective.SetReplicationFactor(policy.GetReplicationFactor())
	}
	if policy.NotificationsEnabled != nil {
		effective.SetNotificationsEnabled(policy.GetNotificationsEnabled())
	}
	if policy.KeySorting != nil {
		effective.SetKeySorting(policy.GetKeySorting())
	}
}

func applyLegacyNamespacePolicy(effective *HierarchyPolicies, namespace *Namespace) {
	if namespace.GetInitialShardCount() != 0 {
		effective.SetInitialShardCount(namespace.GetInitialShardCount())
	}
	if namespace.GetReplicationFactor() != 0 {
		effective.SetReplicationFactor(namespace.GetReplicationFactor())
	}
	if namespace.NotificationsEnabled != nil {
		effective.SetNotificationsEnabled(namespace.GetNotificationsEnabled())
	}
	if namespace.GetKeySorting() != "" {
		effective.SetKeySorting(namespace.GetKeySorting())
	}
}

func MaterializeNamespacePolicy(clusterPolicy *HierarchyPolicies, namespace *Namespace) *Namespace {
	if namespace == nil {
		return nil
	}

	effectivePolicy := ResolveHierarchyPolicies(clusterPolicy, namespace)
	materialized := &Namespace{
		Name:              namespace.GetName(),
		InitialShardCount: effectivePolicy.GetInitialShardCount(),
		ReplicationFactor: effectivePolicy.GetReplicationFactor(),
		KeySorting:        effectivePolicy.GetKeySorting(),
		Policy:            effectivePolicy,
	}
	notificationsEnabled := effectivePolicy.GetNotificationsEnabled()
	materialized.NotificationsEnabled = &notificationsEnabled
	return materialized
}

func (cc *ClusterConfiguration) GetNamespaceEffectivePolicy(namespace *Namespace) *HierarchyPolicies {
	if cc == nil {
		return ResolveHierarchyPolicies(nil, namespace)
	}
	return ResolveHierarchyPolicies(cc.GetPolicy(), namespace)
}

func (cc *ClusterConfiguration) GetNamespaceEffectivePolicyByName(name string) (*HierarchyPolicies, bool) {
	if cc == nil {
		return nil, false
	}
	for _, namespace := range cc.GetNamespaces() {
		if namespace.GetName() == name {
			return cc.GetNamespaceEffectivePolicy(namespace), true
		}
	}
	return nil, false
}

func (cc *ClusterConfiguration) MaterializeNamespace(namespace *Namespace) *Namespace {
	if cc == nil {
		return MaterializeNamespacePolicy(nil, namespace)
	}
	return MaterializeNamespacePolicy(cc.GetPolicy(), namespace)
}

func (ns *Namespace) HasLegacyInitialShardCount() bool {
	return ns != nil && ns.GetInitialShardCount() != 0
}

func (ns *Namespace) HasLegacyKeySorting() bool {
	return ns != nil && ns.GetKeySorting() != ""
}

func (ns *Namespace) LegacyReplicationFactor() uint32 {
	if ns == nil {
		return 0
	}
	return ns.GetReplicationFactor()
}

func (ns *Namespace) LegacyNotificationsEnabled() (value bool, found bool) {
	if ns == nil || ns.NotificationsEnabled == nil {
		return false, false
	}
	return ns.GetNotificationsEnabled(), true
}

func (ns *Namespace) SetLegacyReplicationFactor(value uint32) {
	if ns == nil {
		return
	}
	ns.ReplicationFactor = value
}

func (ns *Namespace) SetLegacyNotificationsEnabled(value bool) {
	if ns == nil {
		return
	}
	ns.NotificationsEnabled = &value
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

	if err := validateHierarchyPolicies("cluster configuration: policy", cc.GetPolicy()); err != nil {
		return err
	}

	for _, ns := range cc.GetNamespaces() {
		if err := validation.ValidateNamespace(ns.GetName()); err != nil {
			return fmt.Errorf("cluster configuration: %w", err)
		}

		if err := validateHierarchyPolicies(fmt.Sprintf("cluster configuration: namespace %q policy", ns.GetName()), ns.GetPolicy()); err != nil {
			return err
		}

		effectivePolicy := cc.GetNamespaceEffectivePolicy(ns)
		if effectivePolicy.GetReplicationFactor() < 1 {
			return fmt.Errorf("cluster configuration: namespace %q has invalid replicationFactor=%d, must be >= 1",
				ns.GetName(), effectivePolicy.GetReplicationFactor())
		}

		if effectivePolicy.GetInitialShardCount() < 1 {
			return fmt.Errorf("cluster configuration: namespace %q has invalid initialShardCount=%d, must be >= 1",
				ns.GetName(), effectivePolicy.GetInitialShardCount())
		}

		keySorting, err := effectivePolicy.GetKeySortingType()
		if err != nil {
			return fmt.Errorf("cluster configuration: namespace %q has invalid keySorting: %w", ns.GetName(), err)
		}
		if keySorting == KeySortingType_UNKNOWN {
			return fmt.Errorf(`cluster configuration: namespace %q has invalid keySorting: must be one of "natural" or "hierarchical"`, ns.GetName())
		}

		if effectivePolicy.GetReplicationFactor() > uint32(len(cc.GetServers())) {
			return fmt.Errorf("cluster configuration: namespace %q has replicationFactor=%d but only %d servers are configured",
				ns.GetName(), effectivePolicy.GetReplicationFactor(), len(cc.GetServers()))
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

func ValidateHierarchyPolicies(policy *HierarchyPolicies) error {
	return validateHierarchyPolicies("hierarchy policy", policy)
}

func validateHierarchyPolicies(prefix string, policy *HierarchyPolicies) error {
	if policy == nil {
		return nil
	}
	if policy.InitialShardCount != nil && policy.GetInitialShardCount() < 1 {
		return fmt.Errorf("%s has invalid initialShardCount=%d, must be >= 1", prefix, policy.GetInitialShardCount())
	}
	if policy.ReplicationFactor != nil && policy.GetReplicationFactor() < 1 {
		return fmt.Errorf("%s has invalid replicationFactor=%d, must be >= 1", prefix, policy.GetReplicationFactor())
	}
	if policy.KeySorting != nil {
		keySorting, err := policy.GetKeySortingType()
		if err != nil {
			return fmt.Errorf("%s has invalid keySorting: %w", prefix, err)
		}
		if keySorting == KeySortingType_UNKNOWN {
			return fmt.Errorf(`%s has invalid keySorting: must be one of "natural" or "hierarchical"`, prefix)
		}
	}
	for i, antiAffinity := range policy.GetAntiAffinities() {
		if antiAffinity.GetModeOrDefault() == AntiAffinityModeUnknown {
			return fmt.Errorf("%s antiAffinities[%d] has invalid mode %q", prefix, i, antiAffinity.GetMode())
		}
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
