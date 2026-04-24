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

func UnmarshalClusterConfigurationYAML(data []byte) (*ClusterConfiguration, error) {
	var generic any
	if err := yaml.Unmarshal(data, &generic); err != nil {
		return nil, err
	}

	jsonBytes, err := json.Marshal(yamlJSONCompatibleValue(generic))
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

func MarshalClusterConfigurationYAML(config *ClusterConfiguration) ([]byte, error) {
	jsonBytes, err := protojson.MarshalOptions{
		UseProtoNames:   false,
		EmitUnpopulated: false,
	}.Marshal(config)
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
		DiscardUnknown: false,
	}).Unmarshal(data, status); err != nil {
		return nil, err
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
