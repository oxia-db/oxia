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

package model

import (
	"github.com/oxia-db/oxia/proto"
)

type PutCall struct {
	Key                string
	Value              []byte
	ExpectedVersionId  *int64
	SequenceKeysDeltas []uint64
	SessionId          *int64
	ClientIdentity     *string
	PartitionKey       *string
	SecondaryIndexes   []*proto.SecondaryIndex
	Callback           func(*proto.PutResponse, error)
}

type DeleteCall struct {
	Key               string
	ExpectedVersionId *int64
	Callback          func(*proto.DeleteResponse, error)
}

type DeleteRangeCall struct {
	MinKeyInclusive string
	MaxKeyExclusive string
	Callback        func(*proto.DeleteRangeResponse, error)
}

type GetCall struct {
	Key                string
	ComparisonType     proto.KeyComparisonType
	IncludeValue       bool
	SecondaryIndexName *string
	Callback           func(*proto.GetResponse, error)
}

func (r PutCall) ToProto() *proto.PutRequest {
	return &proto.PutRequest{
		Key:               r.Key,
		Value:             r.Value,
		ExpectedVersionId: r.ExpectedVersionId,
		SessionId:         r.SessionId,
		ClientIdentity:    r.ClientIdentity,
		PartitionKey:      r.PartitionKey,
		SequenceKeyDelta:  r.SequenceKeysDeltas,
		SecondaryIndexes:  r.SecondaryIndexes,
	}
}

func (r DeleteCall) ToProto() *proto.DeleteRequest {
	return &proto.DeleteRequest{
		Key:               r.Key,
		ExpectedVersionId: r.ExpectedVersionId,
	}
}

func (r DeleteRangeCall) ToProto() *proto.DeleteRangeRequest {
	return &proto.DeleteRangeRequest{
		StartInclusive: r.MinKeyInclusive,
		EndExclusive:   r.MaxKeyExclusive,
	}
}

func (r GetCall) ToProto() *proto.GetRequest {
	return &proto.GetRequest{
		Key:                r.Key,
		ComparisonType:     r.ComparisonType,
		IncludeValue:       r.IncludeValue,
		SecondaryIndexName: r.SecondaryIndexName,
	}
}

func Convert[CALL any, PROTO any](calls []CALL, toProto func(CALL) PROTO) []PROTO {
	protos := make([]PROTO, len(calls))
	for i, call := range calls {
		protos[i] = toProto(call)
	}
	return protos
}
