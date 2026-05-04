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
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v3"
)

func UnmarshalClusterConfigurationJSON(data []byte) (*ClusterConfiguration, error) {
	config := &ClusterConfiguration{}
	if err := (protojson.UnmarshalOptions{
		DiscardUnknown: false,
	}).Unmarshal(data, config); err != nil {
		return nil, err
	}
	return config, nil
}

func UnmarshalClusterConfigurationYAML(data []byte) (*ClusterConfiguration, error) {
	var generic any
	if err := yaml.Unmarshal(data, &generic); err != nil {
		return nil, err
	}

	jsonBytes, err := json.Marshal(migrateClusterConfigurationYAML(yamlJSONCompatibleValue(generic)))
	if err != nil {
		return nil, err
	}

	config := &ClusterConfiguration{}
	if err := (protojson.UnmarshalOptions{
		DiscardUnknown: false,
	}).Unmarshal(jsonBytes, config); err != nil {
		return nil, err
	}

	return config, nil
}

func MarshalClusterConfigurationJSON(config *ClusterConfiguration) ([]byte, error) {
	return protojson.MarshalOptions{
		UseProtoNames:   false,
		EmitUnpopulated: false,
	}.Marshal(config)
}

func MarshalClusterConfigurationYAML(config *ClusterConfiguration) ([]byte, error) {
	jsonBytes, err := MarshalClusterConfigurationJSON(config)
	if err != nil {
		return nil, err
	}

	var generic any
	if err := json.Unmarshal(jsonBytes, &generic); err != nil {
		return nil, err
	}

	return yaml.Marshal(generic)
}

func UnmarshalClusterStatusJSON(data []byte) (*ClusterStatus, error) {
	status := &ClusterStatus{}
	if err := (protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}).Unmarshal(data, status); err != nil {
		return nil, err
	}
	if status.Namespaces == nil {
		status.Namespaces = map[string]*NamespaceStatus{}
	}
	return status, nil
}

func MarshalClusterStatusJSON(status *ClusterStatus) ([]byte, error) {
	return protojson.MarshalOptions{
		UseProtoNames:   false,
		EmitUnpopulated: false,
	}.Marshal(status)
}

func UnmarshalClusterStatusYAML(data []byte) (*ClusterStatus, error) {
	var generic any
	if err := yaml.Unmarshal(data, &generic); err != nil {
		return nil, err
	}

	jsonBytes, err := json.Marshal(yamlJSONCompatibleValue(generic))
	if err != nil {
		return nil, err
	}

	return UnmarshalClusterStatusJSON(jsonBytes)
}

func MarshalClusterStatusYAML(status *ClusterStatus) ([]byte, error) {
	jsonBytes, err := MarshalClusterStatusJSON(status)
	if err != nil {
		return nil, err
	}

	var generic any
	if err := json.Unmarshal(jsonBytes, &generic); err != nil {
		return nil, err
	}

	return yaml.Marshal(generic)
}

func yamlJSONCompatibleValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		converted := make(map[string]any, len(v))
		for key, elem := range v {
			converted[key] = yamlJSONCompatibleValue(elem)
		}
		return converted

	case map[any]any:
		converted := make(map[string]any, len(v))
		for key, elem := range v {
			converted[fmt.Sprint(key)] = yamlJSONCompatibleValue(elem)
		}
		return converted

	case []any:
		converted := make([]any, len(v))
		for i, elem := range v {
			converted[i] = yamlJSONCompatibleValue(elem)
		}
		return converted

	default:
		return v
	}
}

func migrateClusterConfigurationYAML(value any) any {
	config, ok := value.(map[string]any)
	if !ok {
		return value
	}

	namespaces, ok := config["namespaces"].([]any)
	if !ok {
		return value
	}

	for _, item := range namespaces {
		namespace, ok := item.(map[string]any)
		if !ok {
			continue
		}

		policy, ok := namespace["policy"].(map[string]any)
		if !ok {
			policy = map[string]any{}
		}

		changed := false
		changed = moveLegacyNamespacePolicyField(namespace, policy, "initialShardCount", "initial_shard_count") || changed
		changed = moveLegacyNamespacePolicyField(namespace, policy, "replicationFactor", "replication_factor") || changed
		changed = moveLegacyNamespacePolicyField(namespace, policy, "notificationsEnabled", "notifications_enabled") || changed
		changed = moveLegacyNamespacePolicyField(namespace, policy, "keySorting", "key_sorting") || changed
		if changed {
			namespace["policy"] = policy
		}
	}

	return value
}

func moveLegacyNamespacePolicyField(namespace map[string]any, policy map[string]any, jsonName string, protoName string) bool {
	value, found := popAny(namespace, jsonName, protoName)
	if !found {
		return false
	}
	if _, exists := getAny(policy, jsonName, protoName); !exists {
		policy[jsonName] = value
	}
	return true
}

func popAny(values map[string]any, keys ...string) (any, bool) {
	for _, key := range keys {
		value, found := values[key]
		if found {
			delete(values, key)
			return value, true
		}
	}
	return nil, false
}

func getAny(values map[string]any, keys ...string) (any, bool) {
	for _, key := range keys {
		value, found := values[key]
		if found {
			return value, true
		}
	}
	return nil, false
}
