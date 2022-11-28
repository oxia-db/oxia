package server

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"net"
	"oxia/proto"
)

const (
	metadataShardId = "shard-id"
)

type internalRpcServer struct {
	proto.UnimplementedOxiaControlServer
	proto.UnimplementedOxiaLogReplicationServer
	shardsDirector ShardsDirector

	grpcServer *grpc.Server
	port       int
	log        zerolog.Logger
}

func newCoordinationRpcServer(port int, advertisedInternalAddress string, shardsDirector ShardsDirector) (*internalRpcServer, error) {
	res := &internalRpcServer{
		shardsDirector: shardsDirector,
		log: log.With().
			Str("component", "coordination-rpc-server").
			Logger(),
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, errors.Wrap(err, "failed to listen")
	}

	res.port = listener.Addr().(*net.TCPAddr).Port

	res.grpcServer = grpc.NewServer()
	proto.RegisterOxiaControlServer(res.grpcServer, res)
	proto.RegisterOxiaLogReplicationServer(res.grpcServer, res)
	grpc_health_v1.RegisterHealthServer(res.grpcServer, health.NewServer())
	res.log.Info().
		Str("bindAddress", listener.Addr().String()).
		Str("advertisedAddress", advertisedInternalAddress).
		Msg("Started coordination RPC server")

	go func() {
		if err := res.grpcServer.Serve(listener); err != nil {
			log.Fatal().Err(err).Msg("Failed to serve")
		}
	}()

	return res, nil
}

func (s *internalRpcServer) Close() error {
	s.grpcServer.GracefulStop()
	return nil
}

func (s *internalRpcServer) Fence(c context.Context, req *proto.FenceRequest) (*proto.FenceResponse, error) {
	if manager, err := s.shardsDirector.GetManager(req.ShardId, true); err != nil {
		return nil, err
	} else {
		return manager.Fence(req)
	}
}

func (s *internalRpcServer) BecomeLeader(c context.Context, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	if manager, err := s.shardsDirector.GetManager(req.ShardId, true); err != nil {
		return nil, err
	} else {
		return manager.BecomeLeader(req)
	}
}
func (s *internalRpcServer) AddFollower(c context.Context, req *proto.AddFollowerRequest) (*proto.CoordinationEmpty, error) {
	if manager, err := s.shardsDirector.GetManager(req.ShardId, true); err != nil {
		return nil, err
	} else {
		return manager.AddFollower(req)
	}
}
func (s *internalRpcServer) Truncate(c context.Context, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	if manager, err := s.shardsDirector.GetManager(req.ShardId, true); err != nil {
		return nil, err
	} else {
		return manager.Truncate(req)
	}
}
func (s *internalRpcServer) AddEntries(srv proto.OxiaLogReplication_AddEntriesServer) error {
	// Add entries receives an incoming stream of request, the shard_id needs to be encoded
	// as a property in the metadata
	md, ok := metadata.FromIncomingContext(srv.Context())
	if !ok {
		return errors.New("shard id is not set in the request metadata")
	}

	shardId, err := readHeaderUint32(md, metadataShardId)
	if err != nil {
		return err
	}

	if manager, err := s.shardsDirector.GetManager(shardId, true); err != nil {
		return err
	} else {
		return manager.AddEntries(srv)
	}
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

func readHeaderUint32(md metadata.MD, key string) (v uint32, err error) {
	s, err := readHeader(md, key)
	if err != nil {
		return 0, err
	}

	var r uint32
	_, err = fmt.Sscan(s, &r)
	return r, err
}

func (s *internalRpcServer) Port() int {
	return s.port
}

