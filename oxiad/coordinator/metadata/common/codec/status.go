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

func (codec statusCodec) UnmarshalYAML(data []byte) (*commonproto.ClusterStatus, error) {
	jsonBytes, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, err
	}
	return codec.UnmarshalJSON(jsonBytes)
}

func (statusCodec) Clone(value *commonproto.ClusterStatus) *commonproto.ClusterStatus {
	return gproto.Clone(value).(*commonproto.ClusterStatus) //nolint:revive
}

func (statusCodec) NewZero() *commonproto.ClusterStatus {
	return &commonproto.ClusterStatus{}
}

func (statusCodec) MarshalYAML(value *commonproto.ClusterStatus) ([]byte, error) {
	jsonBytes, err := protojson.MarshalOptions{
		UseProtoNames:   false,
		EmitUnpopulated: false,
		UseEnumNumbers:  true,
	}.Marshal(value)
	if err != nil {
		return nil, err
	}

	return yaml.JSONToYAML(jsonBytes)
}

func (statusCodec) UnmarshalJSON(data []byte) (*commonproto.ClusterStatus, error) {
	status := &commonproto.ClusterStatus{}
	if err := (protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}).Unmarshal(data, status); err != nil {
		return nil, err
	}
	return status, nil
}

func (statusCodec) MarshalJSON(value *commonproto.ClusterStatus) ([]byte, error) {
	return protojson.MarshalOptions{
		UseProtoNames:   false,
		EmitUnpopulated: false,
	}.Marshal(value)
}

func (statusCodec) GetKey() string {
	return metadatacommon.ClusterStatusConfigMapDataKey
}
