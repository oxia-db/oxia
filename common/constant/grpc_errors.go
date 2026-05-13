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

package constant

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
)

var (
	ErrCancelled = status.Error(codes.Canceled, "oxia: operation was cancelled")

	ErrInvalidSessionTimeout = status.Error(codes.InvalidArgument, "oxia: invalid session timeout")

	ErrSessionNotFound   = status.Error(codes.NotFound, "oxia: session not found")
	ErrNamespaceNotFound = status.Error(codes.NotFound, "oxia: namespace not found")

	ErrInvalidTerm             = status.Error(codes.FailedPrecondition, "oxia: invalid term")
	ErrInvalidStatus           = status.Error(codes.FailedPrecondition, "oxia: invalid status")
	ErrNotificationsNotEnabled = status.Error(codes.FailedPrecondition, "oxia: notifications not enabled on namespace")

	ErrNodeIsNotMember = status.Error(codes.Aborted, "oxia: node is not a member")
	ErrNodeIsNotLeader = status.Error(codes.Aborted, "oxia: node is not leader")

	ErrNotInitialized      = status.Error(codes.Unavailable, "oxia: server not initialized yet")
	ErrResourceUnavailable = status.Error(codes.Unavailable, "oxia: resource unavailable")
)

func WithLeaderHint(st *status.Status, shard int64, leader string) error {
	if leader == "" {
		return st.Err()
	}
	redirect := &proto.LeaderHint{
		Shard:         shard,
		LeaderAddress: leader,
	}
	stWithDetails, detailsErr := st.WithDetails(redirect)
	if detailsErr != nil {
		return st.Err()
	}
	return stWithDetails.Err()
}

func GetLeaderHint(err error) *proto.LeaderHint {
	st := status.Convert(err)
	if st == nil {
		return nil
	}
	for _, detail := range st.Details() {
		if redirect, ok := detail.(*proto.LeaderHint); ok {
			return redirect
		}
	}
	return nil
}
