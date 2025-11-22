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

package model

import (
	"time"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/entity"
	"github.com/oxia-db/oxia/oxiad/coordinator/policies"
	"github.com/oxia-db/oxia/proto"
)

type ClusterConfig struct {
	Namespaces []NamespaceConfig `json:"namespaces" yaml:"namespaces"`
	Servers    []Server          `json:"servers" yaml:"servers"`
	// ServerMetadata is a map associating server names with their corresponding metadata.
	ServerMetadata map[string]ServerMetadata `json:"serverMetadata" yaml:"serverMetadata"`
	LoadBalancer   *LoadBalancer             `json:"loadBalancer" yaml:"loadBalancer"`
}

type LoadBalancer struct {
	ScheduleInterval time.Duration `json:"scheduleInterval" yaml:"scheduleInterval"`
	QuarantineTime   time.Duration `json:"quarantineTime" yaml:"quarantineTime"`
}

type NamespaceConfig struct {
	Name                 string                       `json:"name" yaml:"name"`
	InitialShardCount    uint32                       `json:"initialShardCount" yaml:"initialShardCount"`
	ReplicationFactor    uint32                       `json:"replicationFactor" yaml:"replicationFactor"`
	NotificationsEnabled entity.OptBooleanDefaultTrue `json:"notificationsEnabled" yaml:"notificationsEnabled"`
	KeySorting           KeySorting                   `json:"keySorting,omitempty" yaml:"keySorting,omitempty"`

	// Policies represents additional configuration policies for the namespace, such as anti-affinity rules.
	Policies *policies.Policies `json:"policies,omitempty" yaml:"policies,omitempty"`
}

type KeySorting string

const (
	KeySortingNatural      KeySorting = "natural"
	KeySortingHierarchical KeySorting = "hierarchical"
)

func (ks *KeySorting) String() string {
	return string(*ks)
}

func (ks *KeySorting) Set(v string) error {
	switch v {
	case string(KeySortingNatural), string(KeySortingHierarchical):
		*ks = KeySorting(v)
		return nil
	default:
		return errors.New(`must be one of "natural" or "hierarchical"`)
	}
}

func (*KeySorting) Type() string {
	return "KeySorting"
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (ks *KeySorting) ToProto() proto.KeySortingType {
	switch *ks {
	case KeySortingNatural:
		return proto.KeySortingType_NATURAL
	case KeySortingHierarchical:
		return proto.KeySortingType_HIERARCHICAL
	default:
		return proto.KeySortingType_UNKNOWN
	}
}
