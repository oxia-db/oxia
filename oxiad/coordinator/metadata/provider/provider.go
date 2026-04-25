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
	"io"
	"strconv"

	"github.com/pkg/errors"

	commonproto "github.com/oxia-db/oxia/common/proto"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"
)

var (
	ErrNotInitialized = errors.New("metadata not initialized")
	ErrBadVersion     = errors.New("metadata bad version")
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

type Provider interface {
	io.Closer

	Get() (cs *commonproto.ClusterStatus, version Version, err error)

	Store(cs *commonproto.ClusterStatus, expectedVersion Version) (newVersion Version, err error)

	WaitToBecomeLeader() error
}

type ClusterConfigStore interface {
	Load() (*commonproto.ClusterConfiguration, error)
	Watch() *commonoption.Watch[*commonproto.ClusterConfiguration]
}

func ParseClusterConfig(data []byte) (*commonproto.ClusterConfiguration, error) {
	config, err := commonproto.UnmarshalClusterConfigurationYAML(data)
	if err != nil {
		return nil, err
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	for _, authority := range config.GetAllowExtraAuthorities() {
		if err := rpc2.ValidateAuthorityAddress(authority); err != nil {
			return nil, errors.Wrapf(err, "cluster configuration: invalid allowExtraAuthorities entry %q", authority)
		}
	}
	return config, nil
}
