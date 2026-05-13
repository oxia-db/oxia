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
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWithLeaderHint(t *testing.T) {
	err := IntoGrpcStatusError(ErrNodeIsNotLeader, WithLeaderHint(1, "leader:6648"))
	oxiaErr, metadata := FromGrpcError(err)
	shard, leader, ok := metadata.GetLeaderHint()

	assert.ErrorIs(t, oxiaErr, ErrNodeIsNotLeader)
	assert.True(t, ok)
	assert.Equal(t, int64(1), shard)
	assert.Equal(t, "leader:6648", leader)
}

func TestIntoGrpcStatusError(t *testing.T) {
	tests := []struct {
		err  error
		code codes.Code
	}{
		{ErrAborted, codes.Aborted},
		{ErrInvalidSessionTimeout, codes.InvalidArgument},
		{ErrSessionNotFound, codes.NotFound},
		{ErrNamespaceNotFound, codes.NotFound},
		{ErrShardNotFound, codes.NotFound},
		{ErrInvalidTerm, codes.FailedPrecondition},
		{ErrInvalidStatus, codes.FailedPrecondition},
		{ErrNotificationsNotEnabled, codes.FailedPrecondition},
		{ErrNodeIsNotMember, codes.Aborted},
		{ErrNodeIsNotLeader, codes.Aborted},
		{ErrNotInitialized, codes.Unavailable},
		{ErrResourceConflict, codes.FailedPrecondition},
		{ErrResourceUnavailable, codes.Unavailable},
	}

	for _, tt := range tests {
		err := IntoGrpcStatusError(tt.err)
		st := status.Convert(err)

		assert.Equal(t, tt.code, st.Code())
		assert.Equal(t, tt.err.Error(), st.Message())
		oxiaErr, metadata := FromGrpcError(err)
		assert.ErrorIs(t, oxiaErr, tt.err)
		assert.Empty(t, metadata)
	}
}

func TestIntoGrpcStatusUnknown(t *testing.T) {
	err := errors.New("other")
	grpcErr := IntoGrpcStatusError(err)
	st := status.Convert(grpcErr)

	assert.Equal(t, codes.Unknown, st.Code())
	assert.Equal(t, err.Error(), st.Message())
	oxiaErr, metadata := FromGrpcError(grpcErr)
	assert.EqualError(t, oxiaErr, grpcErr.Error())
	assert.Empty(t, metadata)
}

func TestIntoGrpcStatusNil(t *testing.T) {
	assert.NoError(t, IntoGrpcStatusError(nil))
}

func TestIntoGrpcStatusReturnsUnknownGrpcStatusError(t *testing.T) {
	err := status.Error(codes.InvalidArgument, "invalid")
	grpcErr := IntoGrpcStatusError(err)
	st := status.Convert(grpcErr)

	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Equal(t, "invalid", st.Message())
}

func TestIntoGrpcStatusAddsErrorInfoToGrpcStatusError(t *testing.T) {
	err := status.Error(codes.Aborted, ErrNodeIsNotLeader.Error())
	grpcErr := IntoGrpcStatusError(err, WithLeaderHint(1, "leader:6648"))
	st := status.Convert(grpcErr)
	oxiaErr, metadata := FromGrpcError(grpcErr)
	shard, leader, ok := metadata.GetLeaderHint()

	assert.Equal(t, codes.Aborted, st.Code())
	assert.Equal(t, ErrNodeIsNotLeader.Error(), st.Message())
	assert.EqualError(t, oxiaErr, grpcErr.Error())
	assert.True(t, ok)
	assert.Equal(t, int64(1), shard)
	assert.Equal(t, "leader:6648", leader)
}

func TestIsRetryable(t *testing.T) {
	assert.True(t, IsRetryable(io.EOF))
	assert.True(t, IsRetryable(ErrAborted))
	assert.True(t, IsRetryable(ErrResourceUnavailable))
	assert.True(t, IsRetryable(ErrNodeIsNotMember))
	assert.True(t, IsRetryable(ErrNodeIsNotLeader))
	assert.True(t, IsRetryable(ErrNotInitialized))

	assert.False(t, IsRetryable(ErrInvalidTerm))
	assert.False(t, IsRetryable(ErrSessionNotFound))
	assert.False(t, IsRetryable(errors.New("other")))
	assert.False(t, IsRetryable(IntoGrpcStatusError(ErrResourceUnavailable)))
}
