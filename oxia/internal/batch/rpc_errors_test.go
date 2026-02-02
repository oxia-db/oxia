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

package batch

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
)

func TestIsRetriable(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retriable bool
	}{
		{
			name:      "nil error",
			err:       nil,
			retriable: false,
		},
		{
			name:      "unavailable error",
			err:       status.Error(codes.Unavailable, "unavailable"),
			retriable: true,
		},
		{
			name:      "invalid status error",
			err:       status.Error(constant.CodeInvalidStatus, "invalid status"),
			retriable: true,
		},
		{
			name:      "already closed error",
			err:       status.Error(constant.CodeAlreadyClosed, "already closed"),
			retriable: true,
		},
		{
			name:      "node is not leader error",
			err:       status.Error(constant.CodeNodeIsNotLeader, "not leader"),
			retriable: true,
		},
		{
			name:      "not found error",
			err:       status.Error(codes.NotFound, "not found"),
			retriable: false,
		},
		{
			name:      "invalid argument error",
			err:       status.Error(codes.InvalidArgument, "invalid argument"),
			retriable: false,
		},
		{
			name:      "internal error",
			err:       status.Error(codes.Internal, "internal error"),
			retriable: false,
		},
		{
			name:      "non-grpc error",
			err:       errors.New("some error"),
			retriable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetriable(tt.err)
			assert.Equal(t, tt.retriable, result)
		})
	}
}

func TestHandleRedirect(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectCallback bool
		expectedShard  int64
		expectedLeader string
	}{
		{
			name:           "nil handler does nothing",
			err:            constant.NewNotLeaderError(1, "leader:6650"),
			expectCallback: false,
		},
		{
			name:           "error with redirect calls handler",
			err:            constant.NewNotLeaderError(5, "new-leader:6650"),
			expectCallback: true,
			expectedShard:  5,
			expectedLeader: "new-leader:6650",
		},
		{
			name:           "error without redirect does not call handler",
			err:            status.Error(constant.CodeNodeIsNotLeader, "not leader"),
			expectCallback: false,
		},
		{
			name:           "error with empty leader address does not call handler",
			err:            constant.NewNotLeaderError(1, ""),
			expectCallback: false,
		},
		{
			name:           "non-grpc error does not call handler",
			err:            errors.New("some error"),
			expectCallback: false,
		},
		{
			name:           "nil error does not call handler",
			err:            nil,
			expectCallback: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var callbackCalled atomic.Bool
			var receivedShard int64
			var receivedLeader string

			handler := func(shardId int64, leaderAddress string) {
				callbackCalled.Store(true)
				receivedShard = shardId
				receivedLeader = leaderAddress
			}

			// Test with nil handler for the first case
			if tt.name == "nil handler does nothing" {
				handleRedirect(tt.err, nil)
				return
			}

			handleRedirect(tt.err, handler)

			if tt.expectCallback {
				assert.True(t, callbackCalled.Load())
				assert.Equal(t, tt.expectedShard, receivedShard)
				assert.Equal(t, tt.expectedLeader, receivedLeader)
			} else {
				assert.False(t, callbackCalled.Load())
			}
		})
	}
}

func TestHandleRedirectWithNilHandler(t *testing.T) {
	// Should not panic with nil handler
	err := constant.NewNotLeaderError(1, "leader:6650")
	assert.NotPanics(t, func() {
		handleRedirect(err, nil)
	})
}
