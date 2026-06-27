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

package oxia

import (
	"context"
	"io"

	"github.com/oxia-db/oxia/common/proto"
)

type AdminClient interface {
	io.Closer

	ListDataServers(ctx context.Context) ([]*proto.DataServer, error)
	GetDataServer(ctx context.Context, dataServer string) (*proto.DataServer, error)
	CreateDataServer(ctx context.Context, dataServer *proto.DataServer) (*proto.DataServer, error)
	PatchDataServer(ctx context.Context, dataServer *proto.DataServer) (*proto.DataServer, error)
	DeleteDataServer(ctx context.Context, dataServer string) (*proto.DataServer, error)

	CreateNamespace(ctx context.Context, namespace *proto.Namespace) (*proto.Namespace, error)
	PatchNamespace(ctx context.Context, namespace *proto.Namespace) (*proto.Namespace, error)
	DeleteNamespace(ctx context.Context, namespace string) (*proto.Namespace, error)
	ListNamespaces(ctx context.Context) ([]*proto.Namespace, error)
	GetNamespace(ctx context.Context, namespace string) (*proto.Namespace, error)

	SplitShard(ctx context.Context, namespace string, shardId int64, splitPoint *uint32) *SplitShardResult
}

type SplitShardResult struct {
	LeftChildShardId  int64
	RightChildShardId int64
	Error             error
}
