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

package internal

import (
	"context"

	"github.com/oxia-db/oxia/common/proto"
)

type Executor interface {
	// ExecuteWrite sends a write request to the leader of the shard. prepare
	// returns the request right before it is handed to gRPC, so that it can
	// leave out the operations that must not be sent anymore, or fail if none
	// is left. Every attempt that gets that far invokes it, and once it
	// returned a request, it must keep returning that one.
	ExecuteWrite(ctx context.Context, shardId int64, prepare func() (*proto.WriteRequest, error)) (*proto.WriteResponse, error)
	ExecuteRead(ctx context.Context, request *proto.ReadRequest) (*proto.ReadResponse, error)
	ExecuteList(ctx context.Context, request *proto.ListRequest, listResponseConsumer func(*proto.ListResponse)) error
	ExecuteRangeScan(ctx context.Context, request *proto.RangeScanRequest, rangeScanResponseConsumer func(*proto.RangeScanResponse)) error
}
