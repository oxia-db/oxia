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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
)

const (
	MaxKeySize              = 10 * 1024       // 10 KB
	MaxValueSize            = 1 * 1024 * 1024 // 1 MB
	MaxPutsPerWrite         = 10_000
	MaxDeletesPerWrite      = 10_000
	MaxDeleteRangesPerWrite = 100
)

func validateWriteRequest(req *proto.WriteRequest) error {
	if len(req.Puts) > MaxPutsPerWrite {
		return status.Errorf(codes.InvalidArgument, "too many puts: %d exceeds maximum %d", len(req.Puts), MaxPutsPerWrite)
	}
	if len(req.Deletes) > MaxDeletesPerWrite {
		return status.Errorf(codes.InvalidArgument, "too many deletes: %d exceeds maximum %d", len(req.Deletes), MaxDeletesPerWrite)
	}
	if len(req.DeleteRanges) > MaxDeleteRangesPerWrite {
		return status.Errorf(codes.InvalidArgument, "too many delete ranges: %d exceeds maximum %d", len(req.DeleteRanges), MaxDeleteRangesPerWrite)
	}
	for _, put := range req.Puts {
		if len(put.Key) > MaxKeySize {
			return status.Errorf(codes.InvalidArgument, "key size %d exceeds maximum %d", len(put.Key), MaxKeySize)
		}
		if len(put.Value) > MaxValueSize {
			return status.Errorf(codes.InvalidArgument, "value size %d exceeds maximum %d", len(put.Value), MaxValueSize)
		}
	}
	for _, del := range req.Deletes {
		if len(del.Key) > MaxKeySize {
			return status.Errorf(codes.InvalidArgument, "key size %d exceeds maximum %d", len(del.Key), MaxKeySize)
		}
	}
	for _, dr := range req.DeleteRanges {
		if len(dr.StartInclusive) > MaxKeySize {
			return status.Errorf(codes.InvalidArgument, "delete range start key size %d exceeds maximum %d", len(dr.StartInclusive), MaxKeySize)
		}
		if len(dr.EndExclusive) > MaxKeySize {
			return status.Errorf(codes.InvalidArgument, "delete range end key size %d exceeds maximum %d", len(dr.EndExclusive), MaxKeySize)
		}
	}
	return nil
}
