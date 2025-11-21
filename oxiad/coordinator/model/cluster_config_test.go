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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClusterConfig(t *testing.T) {
	cc1 := ClusterConfig{
		Namespaces: []NamespaceConfig{{
			Name:              "ns1",
			InitialShardCount: 2,
			ReplicationFactor: 3,
			KeySorting:        KeySortingHierarchical,
		}},
		Servers: []Server{{
			Public:   "f1",
			Internal: "f1",
		}, {
			Public:   "f2",
			Internal: "f2",
		}},
	}

	cc2 := ClusterConfig{
		Namespaces: []NamespaceConfig{{
			Name:              "ns1",
			InitialShardCount: 2,
			ReplicationFactor: 3,
			KeySorting:        KeySortingHierarchical,
		}},
		Servers: []Server{{
			Public:   "f1",
			Internal: "f1",
		}, {
			Public:   "f2",
			Internal: "f2",
		}},
	}

	assert.Equal(t, cc1, cc2)
	assert.NotSame(t, &cc1, &cc2)
}
