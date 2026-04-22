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

package coordinator

import (
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/types/known/durationpb"
	"gopkg.in/yaml.v3"

	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/validation"
	oxiadcommonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"
)

type clusterConfigurationCompat struct {
	Namespaces            []namespaceCompat                   `json:"namespaces" yaml:"namespaces"`
	Servers               []dataServerCompat                  `json:"servers" yaml:"servers"`
	AllowExtraAuthorities []string                            `json:"allowExtraAuthorities,omitempty" yaml:"allowExtraAuthorities,omitempty"`
	ServerMetadata        map[string]dataServerMetadataCompat `json:"serverMetadata" yaml:"serverMetadata"`
	LoadBalancer          *loadBalancerCompat                 `json:"loadBalancer" yaml:"loadBalancer"`
}

type dataServerCompat struct {
	Name     *string `json:"name" yaml:"name"`
	Public   string  `json:"public" yaml:"public"`
	Internal string  `json:"internal" yaml:"internal"`
}

type dataServerMetadataCompat struct {
	Labels map[string]string `json:"labels" yaml:"labels"`
}

type loadBalancerCompat struct {
	ScheduleInterval time.Duration `json:"scheduleInterval" yaml:"scheduleInterval"`
	QuarantineTime   time.Duration `json:"quarantineTime" yaml:"quarantineTime"`
}

type namespaceCompat struct {
	Name                 string                   `json:"name" yaml:"name"`
	InitialShardCount    uint32                   `json:"initialShardCount" yaml:"initialShardCount"`
	ReplicationFactor    uint32                   `json:"replicationFactor" yaml:"replicationFactor"`
	NotificationsEnabled *bool                    `json:"notificationsEnabled,omitempty" yaml:"notificationsEnabled,omitempty"`
	KeySorting           string                   `json:"keySorting,omitempty" yaml:"keySorting,omitempty"`
	HierarchyPolicies    *hierarchyPoliciesCompat `json:"policy,omitempty" yaml:"policy,omitempty"`
}

type hierarchyPoliciesCompat struct {
	AntiAffinities []antiAffinityCompat `json:"antiAffinities,omitempty" yaml:"antiAffinities,omitempty"`
}

type antiAffinityCompat struct {
	Labels []string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Mode   string   `json:"mode,omitempty" yaml:"mode,omitempty"`
}

func decodeClusterConfigurationViper(v *viper.Viper) (*commonproto.ClusterConfiguration, error) {
	var compat clusterConfigurationCompat
	if err := v.Unmarshal(&compat, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
	))); err != nil {
		return nil, errors.Wrap(err, "failed to load cluster configuration")
	}
	return clusterConfigurationFromCompat(&compat)
}

func decodeClusterConfigurationYAML(data []byte) (*commonproto.ClusterConfiguration, error) {
	var compat clusterConfigurationCompat
	if err := yaml.Unmarshal(data, &compat); err != nil {
		return nil, errors.Wrap(err, "failed to decode cluster configuration yaml")
	}
	return clusterConfigurationFromCompat(&compat)
}

func encodeClusterConfigurationYAML(config *commonproto.ClusterConfiguration) ([]byte, error) {
	data, err := yaml.Marshal(clusterConfigurationToCompat(config))
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode cluster configuration yaml")
	}
	return data, nil
}

func clusterConfigurationFromCompat(compat *clusterConfigurationCompat) (*commonproto.ClusterConfiguration, error) {
	config := &commonproto.ClusterConfiguration{
		Namespaces:            make([]*commonproto.Namespace, 0, len(compat.Namespaces)),
		Servers:               make([]*commonproto.DataServer, 0, len(compat.Servers)),
		AllowExtraAuthorities: append([]string(nil), compat.AllowExtraAuthorities...),
		ServerMetadata:        make(map[string]*commonproto.DataServerMetadata, len(compat.ServerMetadata)),
	}

	for _, ns := range compat.Namespaces {
		keySorting, err := commonproto.ParseKeySortingType(ns.KeySorting)
		if err != nil {
			return nil, err
		}
		config.Namespaces = append(config.Namespaces, &commonproto.Namespace{
			Name:                 ns.Name,
			InitialShardCount:    ns.InitialShardCount,
			ReplicationFactor:    ns.ReplicationFactor,
			NotificationsEnabled: ns.NotificationsEnabled,
			KeySorting:           keySorting,
			HierarchyPolicies:    hierarchyPoliciesFromCompat(ns.HierarchyPolicies),
		})
	}

	for _, server := range compat.Servers {
		config.Servers = append(config.Servers, &commonproto.DataServer{
			Name:            server.Name,
			PublicAddress:   server.Public,
			InternalAddress: server.Internal,
		})
	}

	for id, metadata := range compat.ServerMetadata {
		config.ServerMetadata[id] = &commonproto.DataServerMetadata{Labels: metadata.Labels}
	}

	if compat.LoadBalancer != nil {
		config.LoadBalancer = &commonproto.LoadBalancer{
			ScheduleInterval: durationpb.New(compat.LoadBalancer.ScheduleInterval),
			QuarantineTime:   durationpb.New(compat.LoadBalancer.QuarantineTime),
		}
	}

	return config, validateClusterConfiguration(config)
}

func clusterConfigurationToCompat(config *commonproto.ClusterConfiguration) *clusterConfigurationCompat {
	compat := &clusterConfigurationCompat{
		Namespaces:            make([]namespaceCompat, 0, len(config.GetNamespaces())),
		Servers:               make([]dataServerCompat, 0, len(config.GetServers())),
		AllowExtraAuthorities: append([]string(nil), config.GetAllowExtraAuthorities()...),
		ServerMetadata:        make(map[string]dataServerMetadataCompat, len(config.GetServerMetadata())),
	}

	for _, ns := range config.GetNamespaces() {
		var notificationsEnabled *bool
		if ns != nil && ns.NotificationsEnabled != nil {
			value := ns.GetNotificationsEnabled()
			notificationsEnabled = &value
		}
		compat.Namespaces = append(compat.Namespaces, namespaceCompat{
			Name:                 ns.GetName(),
			InitialShardCount:    ns.GetInitialShardCount(),
			ReplicationFactor:    ns.GetReplicationFactor(),
			NotificationsEnabled: notificationsEnabled,
			KeySorting:           keySortingToCompat(ns.GetKeySorting()),
			HierarchyPolicies:    hierarchyPoliciesToCompat(ns.GetHierarchyPolicies()),
		})
	}

	for _, server := range config.GetServers() {
		compat.Servers = append(compat.Servers, dataServerCompat{
			Name:     server.Name,
			Public:   server.GetPublicAddress(),
			Internal: server.GetInternalAddress(),
		})
	}

	for id, metadata := range config.GetServerMetadata() {
		compat.ServerMetadata[id] = dataServerMetadataCompat{Labels: metadata.GetLabels()}
	}

	if loadBalancer := config.GetLoadBalancer(); loadBalancer != nil {
		compat.LoadBalancer = &loadBalancerCompat{
			ScheduleInterval: loadBalancer.GetScheduleInterval().AsDuration(),
			QuarantineTime:   loadBalancer.GetQuarantineTime().AsDuration(),
		}
	}

	return compat
}

func validateClusterConfiguration(config *commonproto.ClusterConfiguration) error {
	if len(config.GetServers()) == 0 {
		return errors.New("cluster configuration: at least one server must be configured")
	}

	if len(config.GetNamespaces()) == 0 {
		return errors.New("cluster configuration: at least one namespace must be configured")
	}

	for _, ns := range config.GetNamespaces() {
		if err := validation.ValidateNamespace(ns.GetName()); err != nil {
			return errors.Wrap(err, "cluster configuration")
		}

		if ns.GetReplicationFactor() < 1 {
			return errors.Errorf("cluster configuration: namespace %q has invalid replicationFactor=%d, must be >= 1",
				ns.GetName(), ns.GetReplicationFactor())
		}

		if ns.GetInitialShardCount() < 1 {
			return errors.Errorf("cluster configuration: namespace %q has invalid initialShardCount=%d, must be >= 1",
				ns.GetName(), ns.GetInitialShardCount())
		}

		if ns.GetReplicationFactor() > uint32(len(config.GetServers())) {
			return errors.Errorf(
				"cluster configuration: namespace %q has replicationFactor=%d but only %d servers are configured",
				ns.GetName(), ns.GetReplicationFactor(), len(config.GetServers()))
		}
	}

	for _, authority := range config.GetAllowExtraAuthorities() {
		if err := oxiadcommonrpc.ValidateAuthorityAddress(authority); err != nil {
			return errors.Wrapf(err, "cluster configuration: invalid allowExtraAuthorities entry %q", authority)
		}
	}

	return nil
}

func keySortingToCompat(value commonproto.KeySortingType) string {
	switch value {
	case commonproto.KeySortingType_NATURAL:
		return "natural"
	case commonproto.KeySortingType_HIERARCHICAL:
		return "hierarchical"
	default:
		return ""
	}
}

func hierarchyPoliciesFromCompat(value *hierarchyPoliciesCompat) *commonproto.HierarchyPolicies {
	if value == nil {
		return nil
	}

	policies := &commonproto.HierarchyPolicies{
		AntiAffinities: make([]*commonproto.AntiAffinity, 0, len(value.AntiAffinities)),
	}
	for _, antiAffinity := range value.AntiAffinities {
		policies.AntiAffinities = append(policies.AntiAffinities, &commonproto.AntiAffinity{
			Labels: antiAffinity.Labels,
			Mode:   antiAffinityModeFromCompat(antiAffinity.Mode),
		})
	}
	return policies
}

func hierarchyPoliciesToCompat(value *commonproto.HierarchyPolicies) *hierarchyPoliciesCompat {
	if value == nil {
		return nil
	}

	policies := &hierarchyPoliciesCompat{
		AntiAffinities: make([]antiAffinityCompat, 0, len(value.GetAntiAffinities())),
	}
	for _, antiAffinity := range value.GetAntiAffinities() {
		policies.AntiAffinities = append(policies.AntiAffinities, antiAffinityCompat{
			Labels: antiAffinity.GetLabels(),
			Mode:   antiAffinityModeToCompat(antiAffinity.GetMode()),
		})
	}
	return policies
}

func antiAffinityModeFromCompat(value string) commonproto.AntiAffinityMode {
	switch value {
	case "strict":
		return commonproto.AntiAffinityMode_ANTI_AFFINITY_MODE_STRICT
	case "relaxed":
		return commonproto.AntiAffinityMode_ANTI_AFFINITY_MODE_RELAXED
	default:
		return commonproto.AntiAffinityMode_ANTI_AFFINITY_MODE_UNKNOWN
	}
}

func antiAffinityModeToCompat(value commonproto.AntiAffinityMode) string {
	switch value {
	case commonproto.AntiAffinityMode_ANTI_AFFINITY_MODE_STRICT:
		return "strict"
	case commonproto.AntiAffinityMode_ANTI_AFFINITY_MODE_RELAXED:
		return "relaxed"
	default:
		return ""
	}
}
