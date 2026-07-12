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

package dataserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"

	oxiadcommonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"

	"github.com/oxia-db/oxia/oxiad/dataserver/assignment"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"

	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"

	"github.com/oxia-db/oxia/common/proto/compat"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/common/channel"

	"github.com/oxia-db/oxia/common/proto"
)

const (
	maxTotalScanBatchCount = 1000
	maxTotalListKeyCount   = 0             // no limitation
	maxTotalListKeySize    = 2 << (10 * 2) // 2Mi
	maxTotalReadCount      = 0
	maxTotalReadValueSize  = 2 << (10 * 2) // 2Mi

	// maxWriteStreamPendingWrites caps the in-flight writes of a single WriteStream:
	// a slot is acquired before a write is submitted to the leader and released only
	// after its response has been sent to the client. Un-sent responses can therefore
	// never exceed the in-flight writes, so the responses channel (same capacity) can
	// always be pushed to without blocking.
	maxWriteStreamPendingWrites = 1024
)

type publicRpcServer struct {
	proto.UnimplementedOxiaClientServer

	shardsDirector             controller.ShardsDirector
	assignmentDispatcher       assignment.ShardAssignmentsDispatcher
	authorityValidationEnabled bool
	grpcServer                 oxiadcommonrpc.GrpcServer
	healthServer               oxiadcommonrpc.HealthServer
	log                        *slog.Logger
}

func newPublicRpcServer(provider oxiadcommonrpc.GrpcProvider, bindAddress string, shardsDirector controller.ShardsDirector, assignmentDispatcher assignment.ShardAssignmentsDispatcher,
	authorityValidationEnabled bool, healthServer oxiadcommonrpc.HealthServer, tlsConf *tls.Config, options *auth.Options) (*publicRpcServer, error) {
	server := &publicRpcServer{
		shardsDirector:             shardsDirector,
		assignmentDispatcher:       assignmentDispatcher,
		authorityValidationEnabled: authorityValidationEnabled,
		healthServer:               healthServer,
		log: slog.With(
			slog.String("component", "public-rpc-server"),
		),
	}

	var err error
	server.grpcServer, err = provider.StartGrpcServer("public", bindAddress, func(registrar grpc.ServiceRegistrar) {
		proto.RegisterOxiaClientServer(registrar, server)
		compat.RegisterOxiaClientServer(registrar, &compatPublicRpcServer{impl: server})
		grpc_health_v1.RegisterHealthServer(registrar, server.healthServer)
	}, tlsConf, options, nil)
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *publicRpcServer) validateAuthority(ctx context.Context) error {
	if !s.authorityValidationEnabled {
		return nil
	}

	if !s.assignmentDispatcher.Initialized() {
		return constant.IntoGrpcStatusError(constant.ErrNotInitialized)
	}

	actualAuthority, err := oxiadcommonrpc.GetAuthority(ctx)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "oxia: invalid authority: %v", err)
	}

	if !s.assignmentDispatcher.HasAuthority(actualAuthority) {
		return status.Errorf(codes.PermissionDenied, "oxia: unexpected authority %q", actualAuthority)
	}

	return nil
}

func (s *publicRpcServer) GetShardAssignments(req *proto.ShardAssignmentsRequest, srv proto.OxiaClient_GetShardAssignmentsServer) error {
	streamContext := srv.Context()
	s.log.Debug(
		"Shard assignments requests",
		slog.String("peer", rpc.GetPeer(streamContext)),
	)
	if err := s.validateAuthority(streamContext); err != nil {
		return err
	}
	err := s.assignmentDispatcher.RegisterForUpdates(req, srv)
	if err != nil {
		s.log.Warn(
			"Failed to add client for shards assignments notifications",
			slog.Any("error", err),
			slog.String("peer", rpc.GetPeer(streamContext)),
		)
		return constant.IntoGrpcStatusError(err)
	}

	return nil
}

func (s *publicRpcServer) Write(ctx context.Context, write *proto.WriteRequest) (*proto.WriteResponse, error) {
	if s.log.Enabled(ctx, slog.LevelDebug) {
		s.log.Debug(
			"Write request",
			slog.String("peer", rpc.GetPeer(ctx)),
			slog.Any("req", write),
		)
	}

	lc, err := s.resolveLeader(ctx, write.Shard)
	if err != nil {
		return nil, err
	}

	wr, err := lc.WriteBlock(ctx, write)
	if err != nil {
		s.log.Warn(
			"Failed to perform write operation",
			slog.Any("error", err),
		)
		return nil, constant.IntoGrpcStatusError(err)
	}

	return wr, err
}

func processWriteStream(streamCtx context.Context, finished chan<- error, stream proto.OxiaClient_WriteStreamServer,
	lc lead.LeaderController, pendingWrites chan<- struct{}, responses chan<- *proto.WriteResponse) {
	for {
		if streamCtx.Err() != nil {
			return
		}

		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				channel.PushNoBlock(finished, nil)
			} else {
				channel.PushNoBlock(finished, err)
			}
			return
		}
		if req == nil {
			channel.PushNoBlock(finished, errors.New("stream closed"))
			return
		}

		select {
		case pendingWrites <- struct{}{}:
		case <-streamCtx.Done():
			channel.PushNoBlock(finished, streamCtx.Err())
			return
		}

		lc.Write(streamCtx, req, concurrent.NewOnce(
			func(t *proto.WriteResponse) {
				// The write callback can be invoked under the quorum-ack-tracker lock:
				// hand the response off to the sender goroutine instead of calling
				// stream.Send here, where a slow client would stall the whole shard.
				// The push cannot block (see maxWriteStreamPendingWrites).
				responses <- t
			}, func(err error) {
				channel.PushNoBlock(finished, err)
			}))
	}
}

func sendWriteStreamResponses(streamCtx context.Context, finished chan<- error, stream proto.OxiaClient_WriteStreamServer,
	pendingWrites <-chan struct{}, responses <-chan *proto.WriteResponse) {
	for {
		select {
		case response := <-responses:
			if err := stream.Send(response); err != nil {
				channel.PushNoBlock(finished, err)
				return
			}
			<-pendingWrites
		case <-streamCtx.Done():
			return
		}
	}
}

func (s *publicRpcServer) WriteStream(stream proto.OxiaClient_WriteStreamServer) error {
	// Add entries receives an incoming stream of request, the shard_id needs to be encoded
	// as a property in the metadata
	md, ok := metadata.FromIncomingContext(stream.Context())
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
	streamCtx := stream.Context()

	log := s.log.With(
		slog.Int64("shard", shardId),
		slog.String("namespace", namespace),
		slog.String("peer", rpc.GetPeer(streamCtx)),
	)
	log.Debug("Write request")

	var lc lead.LeaderController
	lc, err = s.resolveLeader(stream.Context(), &shardId)
	if err != nil {
		return err
	}

	finished := make(chan error, 1)
	pendingWrites := make(chan struct{}, maxWriteStreamPendingWrites)
	responses := make(chan *proto.WriteResponse, maxWriteStreamPendingWrites)
	go process.DoWithLabels(
		streamCtx,
		map[string]string{
			"oxia":      "write-stream",
			"namespace": lc.Namespace(),
			"shard":     fmt.Sprintf("%d", lc.ShardID()),
		},
		func() {
			processWriteStream(streamCtx, finished, stream, lc, pendingWrites, responses)
		},
	)
	go process.DoWithLabels(
		streamCtx,
		map[string]string{
			"oxia":      "write-stream-send",
			"namespace": lc.Namespace(),
			"shard":     fmt.Sprintf("%d", lc.ShardID()),
		},
		func() {
			sendWriteStreamResponses(streamCtx, finished, stream, pendingWrites, responses)
		},
	)

	leaderCtx := lc.Context()
	select {
	case err := <-finished:
		if err != nil {
			s.log.Warn("Failed to perform write operation", slog.Any("error", err))
			return constant.IntoGrpcStatusError(err)
		}
		return nil
	case <-streamCtx.Done():
		return streamCtx.Err()
	// Monitor the leader context to make sure the gRPC server can be gracefully shut down.
	case <-leaderCtx.Done():
		return leaderCtx.Err()
	}
}

// warnOnStreamError logs failures of streaming data operations, except the
// expected ones: the client going away, its deadline expiring, or its
// connection shutting down.
func (s *publicRpcServer) warnOnStreamError(operation string, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isTransportClosingError(err) {
		return
	}
	s.log.Warn("Failed to perform operation", slog.String("operation", operation), slog.Any("error", err))
}

// isTransportClosingError recognizes the status error that a stream send
// returns when the client connection is shutting down. gRPC exposes no
// sentinel for it, so match on code and message.
func isTransportClosingError(err error) bool {
	return status.Code(err) == codes.Unavailable &&
		strings.Contains(status.Convert(err).Message(), "transport is closing")
}

func (s *publicRpcServer) Read(request *proto.ReadRequest, stream proto.OxiaClient_ReadServer) error {
	ctx := stream.Context()
	if s.log.Enabled(ctx, slog.LevelDebug) {
		s.log.Debug(
			"Read request",
			slog.String("peer", rpc.GetPeer(ctx)),
			slog.Any("req", request),
		)
	}

	lc, err := s.resolveLeader(ctx, request.Shard)
	if err != nil {
		return err
	}

	batcher := concurrent.NewBatcher[*proto.GetResponse](maxTotalReadCount, maxTotalReadValueSize,
		func(result *proto.GetResponse) int { return protowire.SizeBytes(len(result.Value)) },
		func(container []*proto.GetResponse) error { return stream.Send(&proto.ReadResponse{Gets: container}) })

	if err = lc.Read(ctx, request, batcher.Add); err == nil {
		err = batcher.Flush()
	}
	if err != nil {
		s.warnOnStreamError("read", err)
		return constant.IntoGrpcStatusError(err)
	}
	return nil
}

func (s *publicRpcServer) List(request *proto.ListRequest, stream proto.OxiaClient_ListServer) error {
	ctx := stream.Context()
	if s.log.Enabled(ctx, slog.LevelDebug) {
		s.log.Debug(
			"List request",
			slog.String("peer", rpc.GetPeer(ctx)),
			slog.Any("req", request),
		)
	}
	lc, err := s.resolveLeader(ctx, request.Shard)
	if err != nil {
		return err
	}
	batcher := concurrent.NewBatcher[string](maxTotalListKeyCount, maxTotalListKeySize,
		func(key string) int { return protowire.SizeBytes(len(key)) },
		func(container []string) error { return stream.Send(&proto.ListResponse{Keys: container}) })

	if err = lc.List(ctx, request, batcher.Add); err == nil {
		err = batcher.Flush()
	}
	if err != nil {
		s.warnOnStreamError("list", err)
		return constant.IntoGrpcStatusError(err)
	}
	return nil
}

func (s *publicRpcServer) RangeScan(request *proto.RangeScanRequest, stream proto.OxiaClient_RangeScanServer) error {
	ctx := stream.Context()
	if s.log.Enabled(ctx, slog.LevelDebug) {
		s.log.Debug(
			"RangeScan request",
			slog.String("peer", rpc.GetPeer(ctx)),
			slog.Any("req", request),
		)
	}

	var lc lead.LeaderController
	var err error
	if lc, err = s.resolveLeader(ctx, request.Shard); err != nil {
		return err
	}

	batcher := concurrent.NewBatcher[*proto.GetResponse](maxTotalScanBatchCount, maxTotalReadValueSize,
		func(response *proto.GetResponse) int { return len(response.Value) },
		func(container []*proto.GetResponse) error {
			return stream.Send(&proto.RangeScanResponse{Records: container})
		})

	if err = lc.RangeScan(ctx, request, batcher.Add); err == nil {
		err = batcher.Flush()
	}
	if err != nil {
		s.warnOnStreamError("range-scan", err)
		return constant.IntoGrpcStatusError(err)
	}
	return nil
}

func (s *publicRpcServer) GetNotifications(req *proto.NotificationsRequest, stream proto.OxiaClient_GetNotificationsServer) error {
	s.log.Debug(
		"Get notifications",
		slog.String("peer", rpc.GetPeer(stream.Context())),
		slog.Any("req", req),
	)

	var lc lead.LeaderController
	var err error
	if lc, err = s.resolveLeader(stream.Context(), &req.Shard); err != nil {
		return err
	}

	ctx := stream.Context()

	finish := make(chan error, 1)
	lc.GetNotifications(ctx, req, concurrent.NewStreamOnce(func(notificationBatch *proto.NotificationBatch) error {
		return stream.Send(notificationBatch)
	}, func(err error) {
		channel.PushNoBlock(finish, err)
	}))

	leaderCtx := lc.Context()
	for {
		select {
		case err := <-finish:
			if err != nil {
				s.log.Warn(
					"Failed to handle notifications request",
					slog.Any("error", err),
				)
				return constant.IntoGrpcStatusError(err)
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-leaderCtx.Done():
			return constant.IntoGrpcStatusError(constant.ErrResourceUnavailable)
		}
	}
}

func (s *publicRpcServer) Port() int {
	return s.grpcServer.Port()
}

func (s *publicRpcServer) CreateSession(ctx context.Context, req *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	s.log.Debug(
		"Create session request",
		slog.String("peer", rpc.GetPeer(ctx)),
		slog.Any("req", req),
	)
	lc, err := s.resolveLeader(ctx, &req.Shard)
	if err != nil {
		return nil, err
	}
	res, err := lc.CreateSession(req)
	if err != nil {
		s.log.Warn(
			"Failed to create session",
			slog.Any("error", err),
		)
		return nil, constant.IntoGrpcStatusError(err)
	}
	return res, nil
}

func (s *publicRpcServer) KeepAlive(ctx context.Context, req *proto.SessionHeartbeat) (*proto.KeepAliveResponse, error) {
	if s.log.Enabled(ctx, slog.LevelDebug) {
		s.log.Debug(
			"Session keep alive",
			slog.Int64("shard", req.Shard),
			slog.Int64("session", req.SessionId),
			slog.String("peer", rpc.GetPeer(ctx)),
		)
	}
	lc, err := s.resolveLeader(ctx, &req.Shard)
	if err != nil {
		return nil, err
	}
	err = lc.KeepAlive(req.SessionId)
	if err != nil {
		s.log.Warn(
			"Failed to listen to heartbeats",
			slog.Any("error", err),
		)
		return nil, constant.IntoGrpcStatusError(err)
	}
	return &proto.KeepAliveResponse{}, nil
}

func (s *publicRpcServer) CloseSession(ctx context.Context, req *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	s.log.Debug(
		"Close session request",
		slog.String("peer", rpc.GetPeer(ctx)),
		slog.Any("req", req),
	)
	lc, err := s.resolveLeader(ctx, &req.Shard)
	if err != nil {
		return nil, err
	}
	res, err := lc.CloseSession(req)
	if err != nil {
		if !errors.Is(err, constant.ErrSessionNotFound) {
			s.log.Warn("Failed to close session", slog.Any("error", err))
		}
		return nil, constant.IntoGrpcStatusError(err)
	}
	return res, nil
}

func (s *publicRpcServer) GetSequenceUpdates(req *proto.GetSequenceUpdatesRequest, stream proto.OxiaClient_GetSequenceUpdatesServer) error {
	s.log.Debug(
		"Get sequence update request",
		slog.String("peer", rpc.GetPeer(stream.Context())),
		slog.Any("req", req),
	)
	lc, err := s.resolveLeader(stream.Context(), &req.Shard)
	if err != nil {
		return err
	}

	ctx := stream.Context()
	sequenceWaiter, err := lc.GetSequenceUpdates(ctx, req)
	if err != nil {
		return constant.IntoGrpcStatusError(err)
	}

	defer func() {
		_ = sequenceWaiter.Close()
	}()

	for {
		select {
		case newKey, more := <-sequenceWaiter.Ch():
			if !more {
				return nil
			}
			if err = stream.Send(&proto.GetSequenceUpdatesResponse{HighestSequenceKey: newKey}); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *publicRpcServer) resolveLeader(ctx context.Context, shardId *int64) (lead.LeaderController, error) {
	if shardId == nil {
		return nil, status.Error(codes.InvalidArgument, "shard id is required")
	}
	if err := s.validateAuthority(ctx); err != nil {
		return nil, err
	}
	shardID := *shardId
	lc, err := s.shardsDirector.GetLeader(shardID)
	if err != nil {
		if errors.Is(err, constant.ErrNodeIsNotLeader) {
			return nil, constant.IntoGrpcStatusError(err, constant.WithLeaderHint(shardID, s.assignmentDispatcher.GetLeader(shardID)))
		}
		s.log.Warn("Failed to get the leader controller", slog.Any("error", err))
		return nil, constant.IntoGrpcStatusError(err)
	}
	return lc, nil
}

func (s *publicRpcServer) Close() error {
	return s.grpcServer.Close()
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type compatPublicRpcServer struct {
	compat.UnimplementedOxiaClientServer
	impl *publicRpcServer
}

func (c compatPublicRpcServer) GetShardAssignments(request *proto.ShardAssignmentsRequest, server compat.OxiaClient_GetShardAssignmentsServer) error {
	return c.impl.GetShardAssignments(request, server)
}

func (c compatPublicRpcServer) Write(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error) {
	return c.impl.Write(ctx, request)
}

func (c compatPublicRpcServer) WriteStream(server compat.OxiaClient_WriteStreamServer) error {
	return c.impl.WriteStream(server)
}

func (c compatPublicRpcServer) Read(request *proto.ReadRequest, server compat.OxiaClient_ReadServer) error {
	return c.impl.Read(request, server)
}

func (c compatPublicRpcServer) List(request *proto.ListRequest, server compat.OxiaClient_ListServer) error {
	return c.impl.List(request, server)
}

func (c compatPublicRpcServer) RangeScan(request *proto.RangeScanRequest, server compat.OxiaClient_RangeScanServer) error {
	return c.impl.RangeScan(request, server)
}

func (c compatPublicRpcServer) GetSequenceUpdates(request *proto.GetSequenceUpdatesRequest, server compat.OxiaClient_GetSequenceUpdatesServer) error {
	return c.impl.GetSequenceUpdates(request, server)
}

func (c compatPublicRpcServer) GetNotifications(request *proto.NotificationsRequest, server compat.OxiaClient_GetNotificationsServer) error {
	return c.impl.GetNotifications(request, server)
}

func (c compatPublicRpcServer) CreateSession(ctx context.Context, request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	return c.impl.CreateSession(ctx, request)
}

func (c compatPublicRpcServer) KeepAlive(ctx context.Context, heartbeat *proto.SessionHeartbeat) (*proto.KeepAliveResponse, error) {
	return c.impl.KeepAlive(ctx, heartbeat)
}

func (c compatPublicRpcServer) CloseSession(ctx context.Context, request *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	return c.impl.CloseSession(ctx, request)
}
