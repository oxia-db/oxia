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

package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/proto"
)

func TestNodeController_HealthCheck(t *testing.T) {
	addr := model.Server{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}

	sap := newMockShardAssignmentsProvider()
	nal := newMockNodeAvailabilityListener()
	rpc := newMockRpcProvider()
	nc := newNodeController(context.Background(), addr, sap, nal, rpc, 1*time.Second)

	assert.Equal(t, Running, nc.Status())

	node := rpc.GetNode(addr)
	node.healthClient.SetStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	unavailableNode := <-nal.events
	assert.Equal(t, addr, unavailableNode)

	assert.Equal(t, NotRunning, nc.Status())

	node.healthClient.SetStatus(grpc_health_v1.HealthCheckResponse_SERVING)

	assert.Eventually(t, func() bool {
		return nc.Status() == Running
	}, 10*time.Second, 100*time.Millisecond)

	node.healthClient.SetError(errors.New("failed to connect"))

	unavailableNode = <-nal.events
	assert.Equal(t, addr, unavailableNode)

	assert.Equal(t, NotRunning, nc.Status())

	assert.NoError(t, nc.Close())
}

func TestNodeController_ShardsAssignments(t *testing.T) {
	addr := model.Server{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}

	sap := newMockShardAssignmentsProvider()
	nal := newMockNodeAvailabilityListener()
	rpc := newMockRpcProvider()
	nc := newNodeController(context.Background(), addr, sap, nal, rpc, 1*time.Second)

	node := rpc.GetNode(addr)

	resp := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{{
					Shard:  0,
					Leader: "leader-0",
				}, {
					Shard:  1,
					Leader: "leader-1",
				}},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}

	sap.set(resp)

	update := <-node.shardAssignmentsStream.updates
	assert.Equal(t, resp, update)

	// Simulate 1 single stream send error
	node.shardAssignmentsStream.SetError(errors.New("failed to send"))

	resp2 := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{{
					Shard:  0,
					Leader: "leader-1",
				}, {
					Shard:  1,
					Leader: "leader-2",
				}},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}

	sap.set(resp2)

	update = <-node.shardAssignmentsStream.updates
	assert.Equal(t, resp2, update)

	assert.NoError(t, nc.Close())
}
