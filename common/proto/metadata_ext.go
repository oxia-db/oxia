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

	AntiAffinityModeUnknown = ""
	AntiAffinityModeStrict  = "strict"
	AntiAffinityModeRelaxed = "relaxed"
)

func (ds *DataServer) GetIdentifier() string {
	if ds == nil {
		return ""
	}
	if ds.Name != nil {
		return ds.GetName()
	}
	return ds.GetInternalAddress()
}

func (ds *DataServer) GetPublicAddress() string {
	if ds == nil {
		return ""
	}
	return ds.GetPublic()
}

func (ds *DataServer) GetInternalAddress() string {
	if ds == nil {
		return ""
	}
	return ds.GetInternal()
}

func (ns *Namespace) NotificationsEnabledOrDefault() bool {
	if ns == nil || ns.NotificationsEnabled == nil {
		return true
	}
	return ns.GetNotificationsEnabled()
}

func (ns *Namespace) GetHierarchyPolicies() *HierarchyPolicies {
	if ns == nil {
		return nil
	}
	return ns.GetPolicy()
}

func (ns *Namespace) KeySortingType() (KeySortingType, error) {
	if ns == nil {
		return KeySortingType_UNKNOWN, nil
	}
	return ParseKeySortingType(ns.GetKeySorting())
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

func (ns *Namespace) KeySortingTypeOrDefault() KeySortingType {
	keySorting, err := ns.KeySortingType()
	if err != nil {
		return KeySortingType_UNKNOWN
	}
	return keySorting
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

func (a *AntiAffinity) ModeOrDefault() string {
	if a == nil {
		return AntiAffinityModeUnknown
	}
	return ParseAntiAffinityMode(a.GetMode())
}

func (cc *ClusterConfiguration) GetDataServerInfo(id string) (*DataServerInfo, bool) {
	if cc == nil {
		return nil, false
	}

	for _, server := range cc.GetServers() {
		if server.GetIdentifier() != id {
			continue
		}

		dataServer := server
		if server.GetName() == "" {
			name := server.GetIdentifier()
			dataServer = &DataServer{
				Name:     &name,
				Public:   server.GetPublicAddress(),
				Internal: server.GetInternalAddress(),
			}
		}

		info := &DataServerInfo{
			DataServer: dataServer,
			Metadata:   &DataServerMetadata{},
		}
		if metadata, found := cc.GetServerMetadata()[id]; found {
			info.Metadata = metadata
		}
		return info, true
	}

	return nil, false
}

func (cc *ClusterConfiguration) Normalize() {
	if cc == nil {
		return
	}

	for _, ns := range cc.GetNamespaces() {
		for _, antiAffinity := range ns.GetHierarchyPolicies().GetAntiAffinities() {
			antiAffinity.Mode = ParseAntiAffinityMode(antiAffinity.GetMode())
		}
	}
}

func (cc *ClusterConfiguration) Validate() error {
	if cc == nil {
		return errors.New("cluster configuration: must not be nil")
	}

	if len(cc.GetServers()) == 0 {
		return errors.New("cluster configuration: at least one server must be configured")
	}

	if len(cc.GetNamespaces()) == 0 {
		return errors.New("cluster configuration: at least one namespace must be configured")
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

		if _, err := ns.KeySortingType(); err != nil {
			return fmt.Errorf("cluster configuration: namespace %q has invalid keySorting: %w", ns.GetName(), err)
		}

		if ns.GetReplicationFactor() > uint32(len(cc.GetServers())) {
			return fmt.Errorf("cluster configuration: namespace %q has replicationFactor=%d but only %d servers are configured",
				ns.GetName(), ns.GetReplicationFactor(), len(cc.GetServers()))
		}
	}

	if loadBalancer := cc.GetLoadBalancer(); loadBalancer != nil {
		if _, err := loadBalancer.ScheduleIntervalDuration(); err != nil {
			return fmt.Errorf("cluster configuration: invalid loadBalancer.scheduleInterval: %w", err)
		}
		if _, err := loadBalancer.QuarantineTimeDuration(); err != nil {
			return fmt.Errorf("cluster configuration: invalid loadBalancer.quarantineTime: %w", err)
		}
	}

	return nil
}

func (lb *LoadBalancer) ScheduleIntervalDuration() (time.Duration, error) {
	if lb == nil || lb.GetScheduleInterval() == "" {
		return 0, nil
	}
	return time.ParseDuration(lb.GetScheduleInterval())
}

func (lb *LoadBalancer) QuarantineTimeDuration() (time.Duration, error) {
	if lb == nil || lb.GetQuarantineTime() == "" {
		return 0, nil
	}
	return time.ParseDuration(lb.GetQuarantineTime())
}

func (lb *LoadBalancer) ScheduleIntervalDurationOrDefault() time.Duration {
	if lb == nil {
		return defaultLoadBalancerScheduleInterval
	}
	duration, err := lb.ScheduleIntervalDuration()
	if err != nil || duration == 0 {
		return defaultLoadBalancerScheduleInterval
	}
	return duration
}

func (lb *LoadBalancer) QuarantineTimeDurationOrDefault() time.Duration {
	if lb == nil {
		return defaultLoadBalancerQuarantineTime
	}
	duration, err := lb.QuarantineTimeDuration()
	if err != nil || duration == 0 {
		return defaultLoadBalancerQuarantineTime
	}
	return duration
}

func (cc *ClusterConfiguration) LoadBalancerWithDefaults() *LoadBalancer {
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
