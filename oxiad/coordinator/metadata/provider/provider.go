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

func (rt ResourceType) Unmarshal(data []byte) (gproto.Message, error) {
	switch rt {
	case ResourceStatus:
		status, err := commonproto.UnmarshalClusterStatusYAML(data)
		if err == nil {
			return status, nil
		}
		legacyStatus, legacyErr := unmarshalLegacyClusterStatus(data)
		if legacyErr == nil {
			return legacyStatus, nil
		}
		return nil, err
	case ResourceConfig:
		return commonproto.UnmarshalClusterConfigurationYAML(data)
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
		return commonproto.MarshalClusterStatusYAML(status)
	case ResourceConfig:
		config, ok := value.(*commonproto.ClusterConfiguration)
		if !ok {
			return nil, fmt.Errorf("expected *ClusterConfiguration for metadata resource type %q, got %T", rt, value)
		}
		return commonproto.MarshalClusterConfigurationYAML(config)
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
		return commonproto.MarshalClusterStatusJSON(status)
	case ResourceConfig:
		config, ok := value.(*commonproto.ClusterConfiguration)
		if !ok {
			return nil, fmt.Errorf("expected *ClusterConfiguration for metadata resource type %q, got %T", rt, value)
		}
		return protojson.MarshalOptions{
			UseProtoNames:   false,
			EmitUnpopulated: false,
		}.Marshal(config)
	default:
		return nil, fmt.Errorf("unknown metadata resource type %q", rt)
	}
}

type Provider interface {
	io.Closer

	Get() (value gproto.Message, version Version, err error)

	Store(value gproto.Message, expectedVersion Version) (newVersion Version, err error)

	WaitToBecomeLeader() error

	Watch() (*metadatawatch.Receiver, error)
}
