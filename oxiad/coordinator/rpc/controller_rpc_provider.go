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

package rpc

import (
	"context"
	"io"

	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/proto"
)

// ControllerRPCProvider is how the coordinator's controllers reach the data
// servers: it sends the OxiaCoordination calls to the data server that node
// identifies, by its internal address. The runtime creates one provider and
// shares it with the data server, shard and split controllers:
//
//	runtime
//	   |
//	   +--> dataserver controller   Handshake, GetHealthClient, PushShardAssignments
//	   +--> shard controller        NewTerm, BecomeLeader, AddFollower, GetStatus, DeleteShard
//	   +--> split controller        the shard calls, plus FreezeShard and RemoveObserver
//
// A call returns the error of the data server as an Oxia error (see
// constant.FromGrpcError), and never retries: the caller decides whether to
// retry it. A unary call is bounded by the timeout of the provider, which is
// common/rpc.DefaultRpcTimeout unless set with
// NewControllerRPCProviderWithTimeout, except BecomeLeader, and by the
// deadline of ctx, whichever is earlier.
type ControllerRPCProvider interface {
	io.Closer

	// PushShardAssignments opens the stream that sends the shard assignments
	// to node. The stream lives as long as ctx: the provider doesn't bound it.
	PushShardAssignments(ctx context.Context, node *proto.DataServerIdentity) (proto.OxiaCoordination_PushShardAssignmentsClient, error)

	// NewTerm fences the replica of the shard on node at the term of req: the
	// node rejects the requests of the older terms, and returns the head
	// entry of its wal, which the election uses to pick the new leader.
	NewTerm(ctx context.Context, node *proto.DataServerIdentity, req *proto.NewTermRequest) (*proto.NewTermResponse, error)

	// BecomeLeader returns once the new leader has committed the entries of its
	// wal through its followers, which can include sending them a snapshot of
	// the whole shard. Only the caller knows how long that can take: the
	// provider doesn't bound it, the caller does through ctx.
	BecomeLeader(ctx context.Context, node *proto.DataServerIdentity, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error)

	// AddFollower makes the leader on node replicate to a follower, or to an
	// observer, from the head entry of the follower's wal.
	AddFollower(ctx context.Context, node *proto.DataServerIdentity, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error)

	// GetStatus returns the term, the role and the offsets of the replica of
	// the shard on node.
	GetStatus(ctx context.Context, node *proto.DataServerIdentity, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error)

	// DeleteShard deletes the replica of the shard on node, and its data.
	DeleteShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)

	// Handshake binds node to the instance of the coordinator, and returns
	// the features it supports. A data server that predates Handshake is
	// asked through the deprecated GetInfo instead, and reported as already
	// bound: the fallback goes away with GetInfo in the next major version.
	Handshake(ctx context.Context, node *proto.DataServerIdentity, req *proto.HandshakeRequest) (*proto.HandshakeResponse, error)

	// RemoveObserver stops the leader on node from replicating to an
	// observer, without fencing the leader: a split that aborts uses it to
	// stop streaming the parent to a child.
	RemoveObserver(ctx context.Context, node *proto.DataServerIdentity, req *proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error)

	// FreezeShard stops the leader on node from accepting writes, or resumes
	// it, without fencing it, so that its observers keep streaming: a split
	// freezes the parent so that the children catch up with its final head
	// offset before the cutover.
	FreezeShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.FreezeShardRequest) (*proto.FreezeShardResponse, error)

	// GetHealthClient returns the client of the gRPC health service of node,
	// which the data server controller probes and watches. It doesn't send
	// any call, and the calls of the client aren't bounded by the timeout of
	// the provider.
	GetHealthClient(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error)
}
