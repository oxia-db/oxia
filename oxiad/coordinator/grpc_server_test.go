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

package coordinator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
)

func TestServerOptions(t *testing.T) {
	so := newServerOptions(nil)
	assert.Nil(t, so.onLeadershipLost, "no handler selects the default")
	assert.Nil(t, so.initialClusterConfig)
	require.NoError(t, so.validate())

	config := &proto.ClusterConfiguration{}
	called := false
	so = newServerOptions([]ServerOption{
		WithOnLeadershipLost(func() { called = true }),
		WithInitialClusterConfiguration(config),
	})

	so.onLeadershipLost()
	assert.True(t, called)
	assert.Same(t, config, so.initialClusterConfig)
	require.NoError(t, so.validate())
}

// A nil option is ignored and a nil leadership-loss handler keeps the default:
// neither can leave the coordinator with a nil to call.
func TestServerOptionsNilSafe(t *testing.T) {
	so := newServerOptions([]ServerOption{nil, WithOnLeadershipLost(nil)})
	assert.Nil(t, so.onLeadershipLost, "a nil handler selects the default")
	assert.Nil(t, so.initialClusterConfig)
}

func TestServerOptionsRejectInvalidInitialClusterConfiguration(t *testing.T) {
	so := newServerOptions([]ServerOption{
		WithInitialClusterConfiguration(&proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{Name: "default", ReplicationFactor: 1}},
		}),
	})
	require.ErrorContains(t, so.validate(), "initialShardCount")
}
