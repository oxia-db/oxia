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

package server

import (
	"context"
	"io"
	"log/slog"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/common/sharding"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/proto"
	"github.com/oxia-db/oxia/server/util"
)

type Client interface {
	Send(*proto.ShardAssignments) error

	Context() context.Context
}

type ShardAssignmentsDispatcher interface {
	io.Closer
	Initialized() bool
	PushShardAssignments(stream proto.OxiaCoordination_PushShardAssignmentsServer) error
	RegisterForUpdates(req *proto.ShardAssignmentsRequest, client Client) error
}

type shardAssignmentDispatcher struct {
	sync.Mutex
	assignments  *proto.ShardAssignments
	clients      map[int64]chan *proto.ShardAssignments
	nextClientId int64
	standalone   bool
	healthServer rpc.HealthServer

	ctx    context.Context
	cancel context.CancelFunc

	log *slog.Logger

	activeClientsGauge metric.Gauge
}

func (s *shardAssignmentDispatcher) RegisterForUpdates(req *proto.ShardAssignmentsRequest, clientStream Client) error {
	s.Lock()

	if s.assignments == nil {
		s.Unlock()
		return constant.ErrNotInitialized
	}

	namespace := req.Namespace
	if namespace == "" {
		namespace = constant.DefaultNamespace
	}

	if _, ok := s.assignments.Namespaces[namespace]; !ok {
		s.Unlock()
		return constant.ErrNamespaceNotFound
	}

	initialAssignments := filterByNamespace(s.assignments, namespace)

	clientCh := make(chan *proto.ShardAssignments)
	clientId := s.nextClientId
	s.nextClientId++

	s.clients[clientId] = clientCh

	assignmentsInterceptorFunc, err := s.assignmentsInterceptorFunc(clientStream)
	if err != nil {
		return err
	}
	s.Unlock()

	// Send initial assignments
	err = clientStream.Send(assignmentsInterceptorFunc(initialAssignments))
	if err != nil {
		s.Lock()
		delete(s.clients, clientId)
		s.Unlock()
		return err
	}

	for {
		select {
		case assignments := <-clientCh:
			if assignments == nil {
				return constant.ErrCancelled
			}

			assignments = filterByNamespace(assignments, namespace)
			err := clientStream.Send(assignmentsInterceptorFunc(assignments))
			if err != nil {
				if status.Code(err) != codes.Canceled {
					peerObject, _ := peer.FromContext(clientStream.Context())
					s.log.Warn(
						"Failed to send shard assignment update to client",
						slog.Any("error", err),
						slog.String("client", peerObject.Addr.String()),
					)
				}
				s.Lock()
				delete(s.clients, clientId)
				s.Unlock()
				return err
			}

		case <-clientStream.Context().Done():
			// The client has disconnected or timed out
			s.Lock()
			delete(s.clients, clientId)
			s.Unlock()
			return nil

		case <-s.ctx.Done():
			// the server is closing
			return nil
		}
	}
}

func filterByNamespace(assignments *proto.ShardAssignments, namespace string) *proto.ShardAssignments {
	filtered := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{},
	}

	for ns, nsa := range assignments.Namespaces {
		if ns == namespace {
			filtered.Namespaces[ns] = nsa
		}
	}

	return filtered
}

func (s *shardAssignmentDispatcher) assignmentsInterceptorFunc(clientStream Client) (func(assignments *proto.ShardAssignments) *proto.ShardAssignments, error) {
	if s.standalone {
		authority, err := authority(clientStream.Context())
		if err != nil {
			return nil, err
		}
		return func(assignments *proto.ShardAssignments) *proto.ShardAssignments {
			assignments = pb.Clone(assignments).(*proto.ShardAssignments) //nolint:revive
			for _, nsa := range assignments.Namespaces {
				for _, assignment := range nsa.Assignments {
					assignment.Leader = authority
				}
			}
			return assignments
		}, nil
	}
	return func(assignments *proto.ShardAssignments) *proto.ShardAssignments {
		return assignments
	}, nil
}

func authority(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		authority := md[":authority"]
		if len(authority) > 0 {
			return authority[0], nil
		}
	}
	return "", status.Errorf(codes.Internal, "oxia: authority not identified")
}

func (s *shardAssignmentDispatcher) Close() error {
	s.activeClientsGauge.Unregister()
	s.cancel()
	return nil
}

func (s *shardAssignmentDispatcher) Initialized() bool {
	s.Lock()
	defer s.Unlock()
	return s.assignments != nil
}

func (s *shardAssignmentDispatcher) PushShardAssignments(stream proto.OxiaCoordination_PushShardAssignmentsServer) error {
	streamReader := util.ReadStream(
		s.ctx,
		stream,
		s.updateShardAssignment,
		map[string]string{
			"oxia": "receive-shards-assignments",
		},
		s.log.With(
			slog.String("stream", "receive-shards-assignments"),
		),
	)
	return streamReader.Run()
}

func (s *shardAssignmentDispatcher) updateShardAssignment(assignments *proto.ShardAssignments) error {
	// Once we receive the first update of the shards mapping, this service can be
	// considered "ready" and it will be able to respond to service discovery requests
	s.healthServer.SetServingStatus(rpc.ReadinessProbeService, grpc_health_v1.HealthCheckResponse_SERVING)

	s.Lock()
	defer s.Unlock()

	s.log.Info("Update shares assignments.",
		slog.Any("previous", s.assignments),
		slog.Any("current", assignments))

	s.assignments = assignments

	// Update all the clients, without getting stuck if any client is not responsive
	for id, clientCh := range s.clients {
		select {
		case clientCh <- assignments:
			// Good, we were able to pass the update to the client

		default:
			// The client is not responsive, cut it off
			close(clientCh)
			delete(s.clients, id)
		}
	}

	return nil
}

func NewShardAssignmentDispatcher(healthServer rpc.HealthServer) ShardAssignmentsDispatcher {
	s := &shardAssignmentDispatcher{
		assignments:  nil,
		healthServer: healthServer,
		clients:      make(map[int64]chan *proto.ShardAssignments),
		log: slog.With(
			slog.String("component", "shard-assignment-dispatcher"),
		),
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.activeClientsGauge = metric.NewGauge("oxia_server_shards_assignments_active_clients",
		"The number of client currently connected for fetching the shards assignments updates", "count",
		map[string]any{}, func() int64 {
			s.Lock()
			defer s.Unlock()

			return int64(len(s.clients))
		})

	return s
}

func NewStandaloneShardAssignmentDispatcher(numShards uint32) ShardAssignmentsDispatcher {
	assignmentDispatcher := NewShardAssignmentDispatcher(rpc.NewCancelableHeathServer(context.Background())).(*shardAssignmentDispatcher) //nolint:revive
	assignmentDispatcher.standalone = true
	res := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
				Assignments:    generateStandaloneShards(numShards),
			},
		},
	}

	err := assignmentDispatcher.updateShardAssignment(res)
	if err != nil {
		panic(err)
	}
	return assignmentDispatcher
}

func generateStandaloneShards(numShards uint32) []*proto.ShardAssignment {
	shards := sharding.GenerateShards(0, numShards)
	assignments := make([]*proto.ShardAssignment, numShards)
	for i, shard := range shards {
		assignments[i] = &proto.ShardAssignment{
			Shard: shard.Id,
			// Leader: defer to send time
			ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
				Int32HashRange: &proto.Int32HashRange{
					MinHashInclusive: shard.Min,
					MaxHashInclusive: shard.Max,
				},
			},
		}
	}
	return assignments
}
