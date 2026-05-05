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

package codec

import (
	"google.golang.org/protobuf/encoding/protojson"
	gproto "google.golang.org/protobuf/proto"
	"sigs.k8s.io/yaml"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
)

func (codec configCodec) UnmarshalYAML(data []byte) (*commonproto.ClusterConfiguration, error) {
	jsonBytes, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, err
	}

	return codec.UnmarshalJSON(jsonBytes)
}

func (configCodec) NewZero() *commonproto.ClusterConfiguration {
	return &commonproto.ClusterConfiguration{}
}

func (configCodec) Clone(value *commonproto.ClusterConfiguration) *commonproto.ClusterConfiguration {
	return gproto.Clone(value).(*commonproto.ClusterConfiguration) //nolint:revive
}

func (codec configCodec) MarshalYAML(value *commonproto.ClusterConfiguration) ([]byte, error) {
	jsonBytes, err := codec.MarshalJSON(value)
	if err != nil {
		return nil, err
	}

	return yaml.JSONToYAML(jsonBytes)
}

func (configCodec) UnmarshalJSON(data []byte) (*commonproto.ClusterConfiguration, error) {
	config := &commonproto.ClusterConfiguration{}
	if err := (protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}).Unmarshal(data, config); err != nil {
		return nil, err
	}
	return config, nil
}

func (configCodec) MarshalJSON(value *commonproto.ClusterConfiguration) ([]byte, error) {
	return protojson.MarshalOptions{
		UseProtoNames:   false,
		EmitUnpopulated: false,
	}.Marshal(value)
}

func (configCodec) GetKey() string {
	return metadatacommon.ClusterConfigConfigMapDataKey
}
