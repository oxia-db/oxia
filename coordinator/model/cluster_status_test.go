// Copyright 2023 StreamNative, Inc.
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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClusterStatus_Clone(t *testing.T) {
	cs1 := &ClusterStatus{
		Namespaces: map[string]NamespaceStatus{
			"test-ns": {
				ReplicationFactor: 3,
				Shards: map[uint32]ShardMetadata{
					0: {
						Status: ShardStatusSteadyState,
						Term:   1,
						Leader: &ServerAddress{
							Public:   "l1",
							Internal: "l1",
						},
						Ensemble: []ServerAddress{{
							Public:   "f1",
							Internal: "f1",
						}, {
							Public:   "f2",
							Internal: "f2",
						}},
						Int32HashRange: Int32HashRange{},
					},
				},
			},
		},
	}

	cs2 := cs1.Clone()

	assert.Equal(t, cs1, cs2)
	assert.NotSame(t, cs1, cs2)
	assert.Equal(t, cs1.Namespaces, cs2.Namespaces)
	assert.NotSame(t, cs1.Namespaces, cs2.Namespaces)
	assert.Equal(t, cs1.Namespaces["test-ns"].Shards, cs2.Namespaces["test-ns"].Shards)
	assert.NotSame(t, cs1.Namespaces["test-ns"].Shards, cs2.Namespaces["test-ns"].Shards)
	assert.Equal(t, cs1.Namespaces["test-ns"].Shards[0], cs2.Namespaces["test-ns"].Shards[0])
	assert.NotSame(t, cs1.Namespaces["test-ns"].Shards[0], cs2.Namespaces["test-ns"].Shards[0])
}
