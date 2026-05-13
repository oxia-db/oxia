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

package internal

import (
	"context"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

type Executor interface {
	ExecuteWrite(ctx context.Context, request *proto.WriteRequest, hint constant.ErrorMetadata) (*proto.WriteResponse, error)
	ExecuteRead(ctx context.Context, request *proto.ReadRequest, hint constant.ErrorMetadata) (proto.OxiaClient_ReadClient, error)
	ExecuteList(ctx context.Context, request *proto.ListRequest, hint constant.ErrorMetadata) (proto.OxiaClient_ListClient, error)
	ExecuteRangeScan(ctx context.Context, request *proto.RangeScanRequest, hint constant.ErrorMetadata) (proto.OxiaClient_RangeScanClient, error)
}
