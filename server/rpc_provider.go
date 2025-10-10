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

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/proto"
)

const rpcTimeout = 30 * time.Second

type ReplicationRpcProvider interface {
	io.Closer
	ReplicateStreamProvider

	Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error)
}

type replicationRpcProvider struct {
	pool rpc.ClientPool
}

func NewReplicationRpcProvider(tlsConf *tls.Config) ReplicationRpcProvider {
	return &replicationRpcProvider{
		pool: rpc.NewClientPool(tlsConf, nil),
	}
}

func (r *replicationRpcProvider) GetReplicateStream(ctx context.Context, follower string, namespace string, shard int64, term int64) (
	proto.OxiaLogReplication_ReplicateClient, error) {
	client, err := r.pool.GetReplicationRpc(follower)
	if err != nil {
		return nil, err
	}

	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataNamespace, namespace)
	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataShardId, fmt.Sprintf("%d", shard))
	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataTerm, fmt.Sprintf("%d", term))

	stream, err := client.Replicate(ctx)
	return stream, err
}

func (r *replicationRpcProvider) SendSnapshot(ctx context.Context, follower string, namespace string, shard int64, term int64) (
	proto.OxiaLogReplication_SendSnapshotClient, error) {
	client, err := r.pool.GetReplicationRpc(follower)
	if err != nil {
		return nil, err
	}

	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataNamespace, namespace)
	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataShardId, fmt.Sprintf("%d", shard))
	ctx = metadata.AppendToOutgoingContext(ctx, constant.MetadataTerm, fmt.Sprintf("%d", term))

	stream, err := client.SendSnapshot(ctx)
	return stream, err
}

func (r *replicationRpcProvider) Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	client, err := r.pool.GetReplicationRpc(follower)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	return client.Truncate(ctx, req)
}

func (r *replicationRpcProvider) Close() error {
	return r.pool.Close()
}
