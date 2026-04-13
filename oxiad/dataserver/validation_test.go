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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
)

func TestValidateWriteRequest_Valid(t *testing.T) {
	req := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "/valid-key", Value: []byte("valid-value")},
		},
		Deletes: []*proto.DeleteRequest{
			{Key: "/delete-key"},
		},
		DeleteRanges: []*proto.DeleteRangeRequest{
			{StartInclusive: "/a", EndExclusive: "/z"},
		},
	}
	assert.NoError(t, validateWriteRequest(req))
}

func TestValidateWriteRequest_KeyTooLarge(t *testing.T) {
	largeKey := strings.Repeat("k", MaxKeySize+1)
	req := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: largeKey, Value: []byte("value")},
		},
	}
	err := validateWriteRequest(req)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "key size")
}

func TestValidateWriteRequest_ValueTooLarge(t *testing.T) {
	largeValue := make([]byte, MaxValueSize+1)
	req := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "/key", Value: largeValue},
		},
	}
	err := validateWriteRequest(req)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "value size")
}

func TestValidateWriteRequest_TooManyPuts(t *testing.T) {
	puts := make([]*proto.PutRequest, MaxPutsPerWrite+1)
	for i := range puts {
		puts[i] = &proto.PutRequest{Key: "/k", Value: []byte("v")}
	}
	req := &proto.WriteRequest{Puts: puts}
	err := validateWriteRequest(req)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "too many puts")
}

func TestValidateWriteRequest_TooManyDeletes(t *testing.T) {
	deletes := make([]*proto.DeleteRequest, MaxDeletesPerWrite+1)
	for i := range deletes {
		deletes[i] = &proto.DeleteRequest{Key: "/k"}
	}
	req := &proto.WriteRequest{Deletes: deletes}
	err := validateWriteRequest(req)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "too many deletes")
}

func TestValidateWriteRequest_DeleteKeyTooLarge(t *testing.T) {
	largeKey := strings.Repeat("k", MaxKeySize+1)
	req := &proto.WriteRequest{
		Deletes: []*proto.DeleteRequest{
			{Key: largeKey},
		},
	}
	err := validateWriteRequest(req)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "key size")
}

func TestValidateWriteRequest_TooManyDeleteRanges(t *testing.T) {
	deleteRanges := make([]*proto.DeleteRangeRequest, MaxDeleteRangesPerWrite+1)
	for i := range deleteRanges {
		deleteRanges[i] = &proto.DeleteRangeRequest{StartInclusive: "/a", EndExclusive: "/z"}
	}
	req := &proto.WriteRequest{DeleteRanges: deleteRanges}
	err := validateWriteRequest(req)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, err.Error(), "too many delete ranges")
}

func TestValidateWriteRequest_DeleteRangeBoundaryKeyTooLarge(t *testing.T) {
	largeKey := strings.Repeat("k", MaxKeySize+1)

	t.Run("start key too large", func(t *testing.T) {
		req := &proto.WriteRequest{
			DeleteRanges: []*proto.DeleteRangeRequest{
				{StartInclusive: largeKey, EndExclusive: "/z"},
			},
		}
		err := validateWriteRequest(req)
		assert.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "delete range start key size")
	})

	t.Run("end key too large", func(t *testing.T) {
		req := &proto.WriteRequest{
			DeleteRanges: []*proto.DeleteRangeRequest{
				{StartInclusive: "/a", EndExclusive: largeKey},
			},
		}
		err := validateWriteRequest(req)
		assert.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "delete range end key size")
	})
}
