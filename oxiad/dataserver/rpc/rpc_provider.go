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

package rpc

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	commonrpc "github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/common/security"
	manifestpkg "github.com/oxia-db/oxia/oxiad/dataserver/manifest"
)

const rpcTimeout = 30 * time.Second

type ReplicateStreamProvider interface {
	GetReplicateStream(ctx context.Context, follower string, namespace string, shard int64, term int64) (proto.OxiaLogReplication_ReplicateClient, error)
	SendSnapshot(ctx context.Context, follower string, namespace string, shard int64, term int64) (proto.OxiaLogReplication_SendSnapshotClient, error)
}

type ReplicationRpcProvider interface {
	io.Closer
	ReplicateStreamProvider

	Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error)
}

type replicationRpcProvider struct {
	pool commonrpc.ClientPool
}

func NewReplicationRpcProvider(tlsOptions *security.TLSOptions, manifest *manifestpkg.Manifest) (ReplicationRpcProvider, error) {
	tlsConf, err := tlsOptions.TryIntoClientTLSConf()
	if err != nil {
		return nil, err
	}
	return &replicationRpcProvider{
		pool: commonrpc.NewClientPool(tlsConf, nil, commonrpc.MetadataInjectionDialOptions(func() map[string]string {
			return map[string]string{
				constant.MetadataInstanceId: manifest.GetInstanceID(),
			}
		})...),
	}, nil
}

func (r *replicationRpcProvider) GetReplicateStream(ctx context.Context, follower string, namespace string, shard int64, term int64) (
	proto.OxiaLogReplication_ReplicateClient, error) {
	client, err := r.pool.GetReplicationRpc(follower)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataNamespace, namespace)
	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataShardId, fmt.Sprintf("%d", shard))
	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataTerm, fmt.Sprintf("%d", term))

	stream, err := client.Replicate(ctx)
	oxiaErr, _ := constant.FromGrpcError(err)
	if oxiaErr != nil {
		return nil, oxiaErr
	}
	return oxiaErrorBidiStreamingClient[proto.Append, proto.Ack]{BidiStreamingClient: stream}, nil
}

func (r *replicationRpcProvider) SendSnapshot(ctx context.Context, follower string, namespace string, shard int64, term int64) (
	proto.OxiaLogReplication_SendSnapshotClient, error) {
	client, err := r.pool.GetReplicationRpc(follower)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataNamespace, namespace)
	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataShardId, fmt.Sprintf("%d", shard))
	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataTerm, fmt.Sprintf("%d", term))

	stream, err := client.SendSnapshot(ctx)
	oxiaErr, _ := constant.FromGrpcError(err)
	if oxiaErr != nil {
		return nil, oxiaErr
	}
	return oxiaErrorClientStreamingClient[proto.SnapshotChunk, proto.SnapshotResponse]{ClientStreamingClient: stream}, nil
}

func (r *replicationRpcProvider) Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	client, err := r.pool.GetReplicationRpc(follower)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	response, err := client.Truncate(ctx, req)
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}

func (r *replicationRpcProvider) Close() error {
	return r.pool.Close()
}

type oxiaErrorBidiStreamingClient[Req any, Res any] struct {
	grpc.BidiStreamingClient[Req, Res]
}

func (c oxiaErrorBidiStreamingClient[Req, Res]) Send(request *Req) error {
	oxiaErr, _ := constant.FromGrpcError(c.BidiStreamingClient.Send(request))
	return oxiaErr
}

func (c oxiaErrorBidiStreamingClient[Req, Res]) Recv() (*Res, error) {
	response, err := c.BidiStreamingClient.Recv()
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}

type oxiaErrorClientStreamingClient[Req any, Res any] struct {
	grpc.ClientStreamingClient[Req, Res]
}

func (c oxiaErrorClientStreamingClient[Req, Res]) Send(request *Req) error {
	oxiaErr, _ := constant.FromGrpcError(c.ClientStreamingClient.Send(request))
	return oxiaErr
}

func (c oxiaErrorClientStreamingClient[Req, Res]) CloseAndRecv() (*Res, error) {
	response, err := c.ClientStreamingClient.CloseAndRecv()
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}
