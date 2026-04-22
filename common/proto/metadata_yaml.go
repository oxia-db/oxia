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
	"fmt"

	"gopkg.in/yaml.v3"
)

func (cc *ClusterConfiguration) UnmarshalYAML(node *yaml.Node) error {
	fields, err := yamlMappingFields(node)
	if err != nil {
		return err
	}

	if value, ok := fields["namespaces"]; ok {
		if err := value.Decode(&cc.Namespaces); err != nil {
			return err
		}
	}
	if value, ok := fields["servers"]; ok {
		if err := value.Decode(&cc.Servers); err != nil {
			return err
		}
	}
	if value, ok := fields["allowExtraAuthorities"]; ok {
		if err := value.Decode(&cc.AllowExtraAuthorities); err != nil {
			return err
		}
	}
	if value, ok := fields["serverMetadata"]; ok {
		if err := value.Decode(&cc.ServerMetadata); err != nil {
			return err
		}
	}
	if value, ok := fields["loadBalancer"]; ok {
		if err := value.Decode(&cc.LoadBalancer); err != nil {
			return err
		}
	}
	return nil
}

func (cc *ClusterConfiguration) MarshalYAML() (any, error) {
	doc := map[string]any{
		"namespaces": cc.GetNamespaces(),
		"servers":    cc.GetServers(),
	}
	if len(cc.GetAllowExtraAuthorities()) > 0 {
		doc["allowExtraAuthorities"] = cc.GetAllowExtraAuthorities()
	}
	if len(cc.GetServerMetadata()) > 0 {
		doc["serverMetadata"] = cc.GetServerMetadata()
	}
	if cc.GetLoadBalancer() != nil {
		doc["loadBalancer"] = cc.GetLoadBalancer()
	}
	return doc, nil
}

func (ns *Namespace) UnmarshalYAML(node *yaml.Node) error {
	fields, err := yamlMappingFields(node)
	if err != nil {
		return err
	}

	if value, ok := fields["name"]; ok {
		if err := value.Decode(&ns.Name); err != nil {
			return err
		}
	}
	if value, ok := fields["initialShardCount"]; ok {
		if err := value.Decode(&ns.InitialShardCount); err != nil {
			return err
		}
	}
	if value, ok := fields["replicationFactor"]; ok {
		if err := value.Decode(&ns.ReplicationFactor); err != nil {
			return err
		}
	}
	if value, ok := fields["notificationsEnabled"]; ok {
		if err := value.Decode(&ns.NotificationsEnabled); err != nil {
			return err
		}
	}
	if value, ok := fields["keySorting"]; ok {
		if err := value.Decode(&ns.KeySorting); err != nil {
			return err
		}
	}
	if value, ok := fields["policy"]; ok {
		if err := value.Decode(&ns.Policy); err != nil {
			return err
		}
	}
	return nil
}

func (ns *Namespace) MarshalYAML() (any, error) {
	doc := map[string]any{
		"name":              ns.GetName(),
		"initialShardCount": ns.GetInitialShardCount(),
		"replicationFactor": ns.GetReplicationFactor(),
	}
	if ns.NotificationsEnabled != nil {
		doc["notificationsEnabled"] = ns.GetNotificationsEnabled()
	}
	if ns.GetKeySorting() != "" {
		doc["keySorting"] = ns.GetKeySorting()
	}
	if ns.GetPolicy() != nil {
		doc["policy"] = ns.GetPolicy()
	}
	return doc, nil
}

func (p *HierarchyPolicies) UnmarshalYAML(node *yaml.Node) error {
	fields, err := yamlMappingFields(node)
	if err != nil {
		return err
	}

	if value, ok := fields["antiAffinities"]; ok {
		if err := value.Decode(&p.AntiAffinities); err != nil {
			return err
		}
	}
	return nil
}

func (p *HierarchyPolicies) MarshalYAML() (any, error) {
	return map[string]any{
		"antiAffinities": p.GetAntiAffinities(),
	}, nil
}

func (lb *LoadBalancer) UnmarshalYAML(node *yaml.Node) error {
	fields, err := yamlMappingFields(node)
	if err != nil {
		return err
	}

	if value, ok := fields["scheduleInterval"]; ok {
		if err := value.Decode(&lb.ScheduleInterval); err != nil {
			return err
		}
	}
	if value, ok := fields["quarantineTime"]; ok {
		if err := value.Decode(&lb.QuarantineTime); err != nil {
			return err
		}
	}
	return nil
}

func (lb *LoadBalancer) MarshalYAML() (any, error) {
	doc := map[string]any{}
	if lb.GetScheduleInterval() != "" {
		doc["scheduleInterval"] = lb.GetScheduleInterval()
	}
	if lb.GetQuarantineTime() != "" {
		doc["quarantineTime"] = lb.GetQuarantineTime()
	}
	return doc, nil
}

func yamlMappingFields(node *yaml.Node) (map[string]*yaml.Node, error) {
	if node.Kind == yaml.DocumentNode && len(node.Content) == 1 {
		node = node.Content[0]
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("expected mapping node but got kind %d", node.Kind)
	}

	fields := make(map[string]*yaml.Node, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		fields[node.Content[i].Value] = node.Content[i+1]
	}
	return fields, nil
}
