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

package provider

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	gproto "google.golang.org/protobuf/proto"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatawatch "github.com/oxia-db/oxia/oxiad/coordinator/metadata/watch"
)

var (
	ErrNotInitialized   = errors.New("metadata not initialized")
	ErrBadVersion       = errors.New("metadata bad version")
	ErrWatchUnsupported = errors.New("metadata watch unsupported")
)

var (
	NameMemory    = "memory"
	NameConfigMap = "configmap"
	NameRaft      = "raft"
	NameFile      = "file"
)

type Version string

const NotExists Version = "-1"

func NextVersion(version Version) Version {
	i, err := strconv.ParseInt(string(version), 10, 64)
	if err != nil {
		return ""
	}

	i++
	return Version(strconv.FormatInt(i, 10))
}

type ResourceType string

const (
	ResourceStatus ResourceType = "status"
	ResourceConfig ResourceType = "config"
)

type WatchMode bool

const (
	WatchDisabled WatchMode = false
	WatchEnabled  WatchMode = true
)

func (wm WatchMode) Enabled() bool {
	return bool(wm)
}

type Codec[T gproto.Message] interface {
	Clone(value T) T
	Unmarshal(data []byte) (T, error)
	MarshalYAML(value T) ([]byte, error)
	MarshalJSON(value T) ([]byte, error)
	ConfigMapDataKey() string
}

type statusCodec struct{}

type configCodec struct{}

var (
	ClusterStatusCodec Codec[*commonproto.ClusterStatus]        = statusCodec{}
	ClusterConfigCodec Codec[*commonproto.ClusterConfiguration] = configCodec{}
)

const (
	ClusterStatusConfigMapDataKey = "status"
	ClusterConfigConfigMapDataKey = "config.yaml"
)

func (statusCodec) Unmarshal(data []byte) (*commonproto.ClusterStatus, error) {
	status, err := commonproto.UnmarshalClusterStatusYAML(data)
	if err == nil {
		return status, nil
	}
	legacyStatus, legacyErr := unmarshalLegacyClusterStatus(data)
	if legacyErr == nil {
		typedStatus, ok := legacyStatus.(*commonproto.ClusterStatus)
		if !ok {
			return nil, fmt.Errorf("expected *ClusterStatus from legacy cluster status container, got %T", legacyStatus)
		}
		return typedStatus, nil
	}
	return nil, err
}

func (statusCodec) Clone(value *commonproto.ClusterStatus) *commonproto.ClusterStatus {
	return cloneMessage(value)
}

func (statusCodec) MarshalYAML(value *commonproto.ClusterStatus) ([]byte, error) {
	return commonproto.MarshalClusterStatusYAML(value)
}

func (statusCodec) MarshalJSON(value *commonproto.ClusterStatus) ([]byte, error) {
	return commonproto.MarshalClusterStatusJSON(value)
}

func (statusCodec) ConfigMapDataKey() string {
	return ClusterStatusConfigMapDataKey
}

func (configCodec) Unmarshal(data []byte) (*commonproto.ClusterConfiguration, error) {
	return commonproto.UnmarshalClusterConfigurationYAML(data)
}

func (configCodec) Clone(value *commonproto.ClusterConfiguration) *commonproto.ClusterConfiguration {
	return cloneMessage(value)
}

func (configCodec) MarshalYAML(value *commonproto.ClusterConfiguration) ([]byte, error) {
	return commonproto.MarshalClusterConfigurationYAML(value)
}

func (configCodec) MarshalJSON(value *commonproto.ClusterConfiguration) ([]byte, error) {
	return protojson.MarshalOptions{
		UseProtoNames:   false,
		EmitUnpopulated: false,
	}.Marshal(value)
}

func (configCodec) ConfigMapDataKey() string {
	return ClusterConfigConfigMapDataKey
}

func (rt ResourceType) Unmarshal(data []byte) (gproto.Message, error) {
	switch rt {
	case ResourceStatus:
		return ClusterStatusCodec.Unmarshal(data)
	case ResourceConfig:
		return ClusterConfigCodec.Unmarshal(data)
	default:
		return nil, fmt.Errorf("unknown metadata resource type %q", rt)
	}
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
		return nil, ErrNotInitialized
	}
	return commonproto.UnmarshalClusterStatusYAML(container.ClusterStatus)
}

func (rt ResourceType) MarshalYAML(value gproto.Message) ([]byte, error) {
	switch rt {
	case ResourceStatus:
		status, ok := value.(*commonproto.ClusterStatus)
		if !ok {
			return nil, fmt.Errorf("expected *ClusterStatus for metadata resource type %q, got %T", rt, value)
		}
		return ClusterStatusCodec.MarshalYAML(status)
	case ResourceConfig:
		config, ok := value.(*commonproto.ClusterConfiguration)
		if !ok {
			return nil, fmt.Errorf("expected *ClusterConfiguration for metadata resource type %q, got %T", rt, value)
		}
		return ClusterConfigCodec.MarshalYAML(config)
	default:
		return nil, fmt.Errorf("unknown metadata resource type %q", rt)
	}
}

func (rt ResourceType) MarshalJSON(value gproto.Message) ([]byte, error) {
	switch rt {
	case ResourceStatus:
		status, ok := value.(*commonproto.ClusterStatus)
		if !ok {
			return nil, fmt.Errorf("expected *ClusterStatus for metadata resource type %q, got %T", rt, value)
		}
		return ClusterStatusCodec.MarshalJSON(status)
	case ResourceConfig:
		config, ok := value.(*commonproto.ClusterConfiguration)
		if !ok {
			return nil, fmt.Errorf("expected *ClusterConfiguration for metadata resource type %q, got %T", rt, value)
		}
		return ClusterConfigCodec.MarshalJSON(config)
	default:
		return nil, fmt.Errorf("unknown metadata resource type %q", rt)
	}
}

type Provider[T gproto.Message] interface {
	io.Closer

	Get() (value T, version Version, err error)

	Store(value T, expectedVersion Version) (newVersion Version, err error)

	WaitToBecomeLeader() error

	Watch() (*metadatawatch.Receiver[T], error)
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
