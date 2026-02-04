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

package dataserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"

	"github.com/oxia-db/oxia/oxiad/common/feature"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"

	"github.com/oxia-db/oxia/oxiad/dataserver/assignment"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller"

	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/common/proto"
)

type internalRpcServer struct {
	proto.UnimplementedOxiaCoordinationServer
	proto.UnimplementedOxiaLogReplicationServer

	shardsDirector       controller.ShardsDirector
	assignmentDispatcher assignment.ShardAssignmentsDispatcher
	grpcServer           rpc2.GrpcServer
	healthServer         rpc2.HealthServer
	log                  *slog.Logger
}

func newInternalRpcServer(grpcProvider rpc2.GrpcProvider, bindAddress string, shardsDirector controller.ShardsDirector,
	assignmentDispatcher assignment.ShardAssignmentsDispatcher, healthServer rpc2.HealthServer, tlsConf *tls.Config) (*internalRpcServer, error) {
	server := &internalRpcServer{
		shardsDirector:       shardsDirector,
		assignmentDispatcher: assignmentDispatcher,
		healthServer:         healthServer,
		log: slog.With(
			slog.String("component", "internal-rpc-server"),
		),
	}

	var err error
	server.grpcServer, err = grpcProvider.StartGrpcServer("internal", bindAddress, func(registrar grpc.ServiceRegistrar) {
		proto.RegisterOxiaCoordinationServer(registrar, server)
		proto.RegisterOxiaLogReplicationServer(registrar, server)
		grpc_health_v1.RegisterHealthServer(registrar, server.healthServer)
	}, tlsConf, &auth.Disabled)
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *internalRpcServer) Close() error {
	return s.grpcServer.Close()
}

func (s *internalRpcServer) PushShardAssignments(srv proto.OxiaCoordination_PushShardAssignmentsServer) error {
	s.log.Info(
		"Received shard assignment request from coordinator",
		slog.String("peer", rpc.GetPeer(srv.Context())),
	)

	err := s.assignmentDispatcher.PushShardAssignments(srv)
	if err != nil && status.Code(err) != codes.Canceled {
		s.log.Warn(
			"Failed to provide shards assignments updates",
			slog.Any("error", err),
			slog.String("peer", rpc.GetPeer(srv.Context())),
		)
	}

	return err
}

func (s *internalRpcServer) NewTerm(c context.Context, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	log := s.log.With(
		slog.Any("req", req),
		slog.String("peer", rpc.GetPeer(c)),
	)

	log.Info("Received NewTerm request")

	// NewTerm applies to both followers and leaders
	// First check if we have already a follower controller running
	if follower, err := s.shardsDirector.GetFollower(req.Shard); err != nil { //nolint:revive
		if status.Code(err) != constant.CodeNodeIsNotFollower {
			log.Warn(
				"NewTerm failed: could not get follower controller",
				slog.Any("error", err),
			)
			return nil, err
		}
		log.Debug(
			"Node is not follower, getting leader",
			slog.Any("error", err),
		)

		// If we don't have a follower, fallback to checking the leader controller
	} else {
		log.Info(
			"Found follower, initiating new term",
			slog.Int64("followerTerm", follower.Term()),
		)
		res, err2 := follower.NewTerm(req)
		if err2 != nil {
			log.Warn(
				"NewTerm of follower failed",
				slog.Any("error", err2),
			)
		}
		return res, err2
	}

	leader, err := s.shardsDirector.GetOrCreateLeader(req.Namespace, req.Shard, req.Options)
	if err != nil {
		log.Warn(
			"NewTerm failed: could not get leader controller",
			slog.Any("error", err),
		)
		return nil, err
	}
	res, err2 := leader.NewTerm(req)
	if err2 != nil {
		log.Warn(
			"New term processing of leader failed",
			slog.Any("error", err2),
			slog.Int64("leaderTerm", leader.Term()),
		)
	} else {
		log.Info(
			"New term processing completed",
			slog.Any("response", res),
			slog.Int64("leaderTerm", leader.Term()),
		)
	}
	return res, err2
}

func (s *internalRpcServer) BecomeLeader(c context.Context, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	log := s.log.With(
		slog.Any("request", req),
		slog.String("peer", rpc.GetPeer(c)),
	)

	log.Info("Received BecomeLeader request")

	leader, err := s.shardsDirector.GetOrCreateLeader(req.Namespace, req.Shard, nil)
	if err != nil {
		log.Warn(
			"BecomeLeader failed: could not get leader controller",
			slog.Any("error", err),
		)
		return nil, err
	}

	res, err := leader.BecomeLeader(c, req)
	if err != nil {
		log.Warn(
			"BecomeLeader failed",
			slog.Any("error", err),
		)
	}
	return res, err
}

func (s *internalRpcServer) AddFollower(c context.Context, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	log := s.log.With(
		slog.Any("request", req),
		slog.String("peer", rpc.GetPeer(c)),
	)

	log.Info("Received AddFollower request")

	leader, err := s.shardsDirector.GetLeader(req.Shard)
	if err != nil {
		log.Warn(
			"AddFollower failed: could not get leader controller",
			slog.Any("error", err),
		)
		return nil, err
	}

	res, err := leader.AddFollower(req)
	if err != nil {
		log.Warn(
			"AddFollower failed",
			slog.Any("error", err),
		)
	}
	return res, err
}

func (s *internalRpcServer) GetInfo(c context.Context, req *proto.GetInfoRequest) (*proto.GetInfoResponse, error) {
	log := s.log.With(
		slog.Any("request", req),
		slog.String("peer", rpc.GetPeer(c)),
	)
	log.Info("Received GetInfo request")
	features := feature.SupportedFeatures()
	return &proto.GetInfoResponse{
		FeaturesSupported: features,
	}, nil
}

func (s *internalRpcServer) Truncate(c context.Context, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	log := s.log.With(
		slog.Any("request", req),
		slog.String("peer", rpc.GetPeer(c)),
	)

	log.Info("Received Truncate request")

	follower, err := s.shardsDirector.GetOrCreateFollower(req.Namespace, req.Shard, req.Term, nil)
	if err != nil {
		log.Warn(
			"Truncate failed: could not get follower controller",
			slog.Any("error", err),
		)
		return nil, err
	}

	res, err := follower.Truncate(req)
	if err != nil {
		log.Warn(
			"Truncate failed",
			slog.Any("error", err),
		)
	}
	return res, err
}

func (s *internalRpcServer) Replicate(srv proto.OxiaLogReplication_ReplicateServer) error {
	// Add entries receives an incoming stream of request, the shard_id needs to be encoded
	// as a property in the metadata
	md, ok := metadata.FromIncomingContext(srv.Context())
	if !ok {
		return errors.New("shard id is not set in the request metadata")
	}

	shardId, err := ReadHeaderInt64(md, constant.MetadataShardId)
	if err != nil {
		return err
	}

	namespace, err := readHeader(md, constant.MetadataNamespace)
	if err != nil {
		return err
	}

	term, err := readTerm(md)
	if err != nil {
		return err
	}

	log := s.log.With(
		slog.Int64("shard", shardId),
		slog.String("namespace", namespace),
		slog.String("peer", rpc.GetPeer(srv.Context())),
	)

	log.Info("Received Replicate request")

	follower, err := s.shardsDirector.GetOrCreateFollower(namespace, shardId, term, nil)
	if err != nil {
		log.Warn(
			"Replicate failed: could not get follower controller",
			slog.Any("error", err),
		)
		return err
	}

	err = follower.Replicate(srv)
	if err != nil && !errors.Is(err, io.EOF) {
		log.Warn(
			"Replicate failed",
			slog.Any("error", err),
		)
	}
	return err
}

func (s *internalRpcServer) SendSnapshot(srv proto.OxiaLogReplication_SendSnapshotServer) error {
	// Send snapshot receives an incoming stream of requests, the shard_id needs to be encoded
	// as a property in the metadata
	md, ok := metadata.FromIncomingContext(srv.Context())
	if !ok {
		return errors.New("shard id is not set in the request metadata")
	}

	shardId, err := ReadHeaderInt64(md, constant.MetadataShardId)
	if err != nil {
		return err
	}

	namespace, err := readHeader(md, constant.MetadataNamespace)
	if err != nil {
		return err
	}

	term, err := readTerm(md)
	if err != nil {
		return err
	}

	s.log.Info(
		"Received SendSnapshot request",
		slog.Int64("shard", shardId),
		slog.String("namespace", namespace),
		slog.String("peer", rpc.GetPeer(srv.Context())),
	)

	follower, err := s.shardsDirector.GetOrCreateFollower(namespace, shardId, term, nil)
	if err != nil {
		s.log.Warn(
			"SendSnapshot failed: could not get follower controller",
			slog.Any("error", err),
			slog.String("namespace", namespace),
			slog.Int64("shard", shardId),
			slog.String("peer", rpc.GetPeer(srv.Context())),
		)
		return err
	}

	err = follower.SendSnapshot(srv)
	if err != nil {
		s.log.Warn(
			"SendSnapshot failed",
			slog.Any("error", err),
			slog.String("namespace", namespace),
			slog.Int64("shard", shardId),
			slog.String("peer", rpc.GetPeer(srv.Context())),
		)
	}
	return err
}

func (s *internalRpcServer) GetStatus(_ context.Context, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	follower, err := s.shardsDirector.GetFollower(req.Shard)
	if err == nil {
		return follower.GetStatus(req)
	}

	if status.Code(err) != constant.CodeNodeIsNotFollower {
		return nil, err
	}

	// If we don't have a follower, fallback to checking the leader controller
	leader, err := s.shardsDirector.GetLeader(req.Shard)
	if err != nil {
		return nil, err
	}

	return leader.GetStatus(req)
}

func (s *internalRpcServer) DeleteShard(_ context.Context, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	return s.shardsDirector.DeleteShard(req)
}

func readHeader(md metadata.MD, key string) (value string, err error) {
	arr := md.Get(key)
	if len(arr) == 0 {
		return "", errors.Errorf("Request must include '%s' metadata field", key)
	}

	if len(arr) > 1 {
		return "", errors.Errorf("Request must include '%s' metadata field only once", key)
	}
	return arr[0], nil
}

func ReadHeaderInt64(md metadata.MD, key string) (v int64, err error) {
	s, err := readHeader(md, key)
	if err != nil {
		return 0, err
	}

	var r int64
	_, err = fmt.Sscan(s, &r)
	return r, err
}

func readTerm(md metadata.MD) (v int64, err error) {
	arr := md.Get(constant.MetadataTerm)
	if len(arr) == 0 {
		// There was no term in the metadata for the stream.
		// In order to retain compatibility in a rollout scenario, skip
		// the term check
		return -1, nil
	}

	return ReadHeaderInt64(md, constant.MetadataTerm)
}
