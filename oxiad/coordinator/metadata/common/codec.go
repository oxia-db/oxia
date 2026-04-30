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

package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	gproto "google.golang.org/protobuf/proto"

	commonproto "github.com/oxia-db/oxia/common/proto"
)

type Codec[T gproto.Message] interface {
	Clone(value T) T
	NewZero() T
	UnmarshalYAML(data []byte) (T, error)
	MarshalYAML(value T) ([]byte, error)
	UnmarshalJSON(data []byte) (T, error)
	MarshalJSON(value T) ([]byte, error)
	GetKey() string
}

type statusCodec struct{}

type configCodec struct{}

var (
	ClusterStatusCodec Codec[*commonproto.ClusterStatus]        = statusCodec{}
	ClusterConfigCodec Codec[*commonproto.ClusterConfiguration] = configCodec{}
)

func (statusCodec) UnmarshalYAML(data []byte) (*commonproto.ClusterStatus, error) {
	legacyStatus, legacyErr := unmarshalLegacyClusterStatus(data)
	if legacyErr == nil {
		typedStatus, ok := legacyStatus.(*commonproto.ClusterStatus)
		if !ok {
			return nil, fmt.Errorf("expected *ClusterStatus from legacy cluster status container, got %T", legacyStatus)
		}
		return typedStatus, nil
	}
	return commonproto.UnmarshalClusterStatusYAML(data)
}

func (statusCodec) Clone(value *commonproto.ClusterStatus) *commonproto.ClusterStatus {
	return cloneMessage(value)
}

func (statusCodec) NewZero() *commonproto.ClusterStatus {
	return &commonproto.ClusterStatus{}
}

func (statusCodec) MarshalYAML(value *commonproto.ClusterStatus) ([]byte, error) {
	return commonproto.MarshalClusterStatusYAML(value)
}

func (statusCodec) UnmarshalJSON(data []byte) (*commonproto.ClusterStatus, error) {
	return commonproto.UnmarshalClusterStatusJSON(data)
}

func (statusCodec) MarshalJSON(value *commonproto.ClusterStatus) ([]byte, error) {
	return commonproto.MarshalClusterStatusJSON(value)
}

func (statusCodec) GetKey() string {
	return ClusterStatusConfigMapDataKey
}

func (configCodec) UnmarshalYAML(data []byte) (*commonproto.ClusterConfiguration, error) {
	return commonproto.UnmarshalClusterConfigurationYAML(data)
}

func (configCodec) NewZero() *commonproto.ClusterConfiguration {
	return &commonproto.ClusterConfiguration{}
}

func (configCodec) Clone(value *commonproto.ClusterConfiguration) *commonproto.ClusterConfiguration {
	return cloneMessage(value)
}

func (configCodec) MarshalYAML(value *commonproto.ClusterConfiguration) ([]byte, error) {
	return commonproto.MarshalClusterConfigurationYAML(value)
}

func (configCodec) UnmarshalJSON(data []byte) (*commonproto.ClusterConfiguration, error) {
	return commonproto.UnmarshalClusterConfigurationJSON(data)
}

func (configCodec) MarshalJSON(value *commonproto.ClusterConfiguration) ([]byte, error) {
	return commonproto.MarshalClusterConfigurationJSON(value)
}

func (configCodec) GetKey() string {
	return ClusterConfigConfigMapDataKey
}

type legacyClusterStatusContainer struct {
	ClusterStatus json.RawMessage `json:"clusterStatus,omitempty"`
}

func unmarshalLegacyClusterStatus(data []byte) (gproto.Message, error) {
	container := legacyClusterStatusContainer{}
	if err := json.Unmarshal(data, &container); err != nil {
		return nil, err
	}
	if len(container.ClusterStatus) == 0 {
		return nil, errors.New("metadata not initialized")
	}
	return commonproto.UnmarshalClusterStatusYAML(container.ClusterStatus)
}

func cloneMessage[T gproto.Message](value T) T {
	var zero T
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return zero
	}
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return zero
		}
	default:
	}
	cloned, ok := gproto.Clone(value).(T)
	if !ok {
		panic(fmt.Sprintf("failed to clone metadata value of type %T", value))
	}
	return cloned
}
