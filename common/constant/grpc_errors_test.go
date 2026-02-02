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
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
)

func TestNewNotLeaderError(t *testing.T) {
	tests := []struct {
		name          string
		shardId       int64
		leaderAddress string
		expectDetails bool
	}{
		{
			name:          "with leader address",
			shardId:       1,
			leaderAddress: "leader:6650",
			expectDetails: true,
		},
		{
			name:          "with empty leader address",
			shardId:       2,
			leaderAddress: "",
			expectDetails: false,
		},
		{
			name:          "with different shard",
			shardId:       100,
			leaderAddress: "other-leader:6650",
			expectDetails: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewNotLeaderError(tt.shardId, tt.leaderAddress)

			assert.Error(t, err)
			assert.Equal(t, CodeNodeIsNotLeader, status.Code(err))

			redirect := ExtractServerRedirect(err)
			if tt.expectDetails {
				assert.NotNil(t, redirect)
				assert.Equal(t, tt.shardId, redirect.Shard)
				assert.Equal(t, tt.leaderAddress, redirect.LeaderAddress)
			} else {
				assert.Nil(t, redirect)
			}
		})
	}
}

func TestExtractServerRedirect(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectRedirect bool
		expectedShard  int64
		expectedLeader string
	}{
		{
			name:           "nil error",
			err:            nil,
			expectRedirect: false,
		},
		{
			name:           "non-grpc error",
			err:            errors.New("some error"),
			expectRedirect: false,
		},
		{
			name:           "grpc error without details",
			err:            status.Error(CodeNodeIsNotLeader, "not leader"),
			expectRedirect: false,
		},
		{
			name: "grpc error with redirect details",
			err: func() error {
				st := status.New(CodeNodeIsNotLeader, "not leader")
				st, _ = st.WithDetails(&proto.ServerRedirect{
					Shard:         5,
					LeaderAddress: "new-leader:6650",
				})
				return st.Err()
			}(),
			expectRedirect: true,
			expectedShard:  5,
			expectedLeader: "new-leader:6650",
		},
		{
			name:           "grpc error with different code",
			err:            status.Error(codes.Unavailable, "unavailable"),
			expectRedirect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			redirect := ExtractServerRedirect(tt.err)

			if tt.expectRedirect {
				assert.NotNil(t, redirect)
				assert.Equal(t, tt.expectedShard, redirect.Shard)
				assert.Equal(t, tt.expectedLeader, redirect.LeaderAddress)
			} else {
				assert.Nil(t, redirect)
			}
		})
	}
}

func TestNewNotLeaderErrorRoundTrip(t *testing.T) {
	// Test that we can create an error and extract the redirect from it
	shardId := int64(42)
	leaderAddress := "leader-host:6650"

	err := NewNotLeaderError(shardId, leaderAddress)

	// Verify error properties
	assert.Error(t, err)
	assert.Equal(t, CodeNodeIsNotLeader, status.Code(err))
	assert.Contains(t, err.Error(), "42")

	// Extract and verify redirect
	redirect := ExtractServerRedirect(err)
	assert.NotNil(t, redirect)
	assert.Equal(t, shardId, redirect.Shard)
	assert.Equal(t, leaderAddress, redirect.LeaderAddress)
}
