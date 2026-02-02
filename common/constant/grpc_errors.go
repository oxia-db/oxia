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
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
)

const (
	CodeNotInitialized          codes.Code = 100
	CodeInvalidTerm             codes.Code = 101
	CodeInvalidStatus           codes.Code = 102
	CodeCancelled               codes.Code = 103
	CodeAlreadyClosed           codes.Code = 104
	CodeLeaderAlreadyConnected  codes.Code = 105
	CodeNodeIsNotLeader         codes.Code = 106
	CodeNodeIsNotFollower       codes.Code = 107
	CodeSessionNotFound         codes.Code = 108
	CodeInvalidSessionTimeout   codes.Code = 109
	CodeNamespaceNotFound       codes.Code = 110
	CodeNotificationsNotEnabled codes.Code = 111
)

var (
	ErrNotInitialized          = status.Error(CodeNotInitialized, "oxia: server not initialized yet")
	ErrCancelled               = status.Error(CodeCancelled, "oxia: operation was cancelled")
	ErrInvalidTerm             = status.Error(CodeInvalidTerm, "oxia: invalid term")
	ErrInvalidStatus           = status.Error(CodeInvalidStatus, "oxia: invalid status")
	ErrLeaderAlreadyConnected  = status.Error(CodeLeaderAlreadyConnected, "oxia: leader is already connected")
	ErrAlreadyClosed           = status.Error(CodeAlreadyClosed, "oxia: resource is already closed")
	ErrSessionNotFound         = status.Error(CodeSessionNotFound, "oxia: session not found")
	ErrInvalidSessionTimeout   = status.Error(CodeInvalidSessionTimeout, "oxia: invalid session timeout")
	ErrNamespaceNotFound       = status.Error(CodeNamespaceNotFound, "oxia: namespace not found")
	ErrNotificationsNotEnabled = status.Error(CodeNotificationsNotEnabled, "oxia: notifications not enabled on namespace")
)

// NewNotLeaderError creates a gRPC error with CodeNodeIsNotLeader and embeds
// the leader address in the error details so clients can retry directly to the
// correct leader.
func NewNotLeaderError(shardId int64, leaderAddress string) error {
	st := status.New(CodeNodeIsNotLeader, fmt.Sprintf("node is not leader for shard %d", shardId))
	if leaderAddress != "" {
		redirect := &proto.ServerRedirect{
			Shard:         shardId,
			LeaderAddress: leaderAddress,
		}
		stWithDetails, err := st.WithDetails(redirect)
		if err != nil {
			// If we fail to add details, return the original error
			return st.Err()
		}
		return stWithDetails.Err()
	}
	return st.Err()
}

// ExtractServerRedirect extracts the ServerRedirect details from a gRPC error
// if present. Returns nil if the error doesn't contain redirect info.
func ExtractServerRedirect(err error) *proto.ServerRedirect {
	st := status.Convert(err)
	if st == nil {
		return nil
	}
	for _, detail := range st.Details() {
		if redirect, ok := detail.(*proto.ServerRedirect); ok {
			return redirect
		}
	}
	return nil
}
