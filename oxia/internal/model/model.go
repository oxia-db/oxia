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

package model

import (
	"cmp"
	"slices"

	"github.com/oxia-db/oxia/common/proto"
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
	OpIndex            uint32 // Position in the write batch, set when added to it
	Callback           func(*proto.PutResponse, error)
}

type DeleteCall struct {
	Key               string
	ExpectedVersionId *int64
	PartitionKey      *string
	OpIndex           uint32 // Position in the write batch, set when added to it
	Callback          func(*proto.DeleteResponse, error)
}

type DeleteRangeCall struct {
	MinKeyInclusive string
	MaxKeyExclusive string
	OpIndex         uint32 // Position in the write batch, set when added to it
	Callback        func(*proto.DeleteRangeResponse, error)
}

type GetCall struct {
	Key                string
	ComparisonType     proto.KeyComparisonType
	IncludeValue       bool
	SecondaryIndexName *string
	PartitionKey       *string
	Callback           func(*proto.GetResponse, error)
}

// PartitionKeyOrKey returns the partition key if set, otherwise the record key.
// Used for shard routing when re-routing operations after a shard split.
func (r GetCall) PartitionKeyOrKey() string {
	if r.PartitionKey != nil {
		return *r.PartitionKey
	}
	return r.Key
}

// PartitionKeyOrKey returns the partition key if set, otherwise the record key.
// Used for shard routing when re-routing operations after a shard split.
func (r PutCall) PartitionKeyOrKey() string {
	if r.PartitionKey != nil {
		return *r.PartitionKey
	}
	return r.Key
}

// PartitionKeyOrKey returns the partition key if set, otherwise the record key.
// Used for shard routing when re-routing operations after a shard split.
func (r DeleteCall) PartitionKeyOrKey() string {
	if r.PartitionKey != nil {
		return *r.PartitionKey
	}
	return r.Key
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
		OpIndex:           r.OpIndex,
	}
}

func (r DeleteCall) ToProto() *proto.DeleteRequest {
	return &proto.DeleteRequest{
		Key:               r.Key,
		ExpectedVersionId: r.ExpectedVersionId,
		PartitionKey:      r.PartitionKey,
		OpIndex:           r.OpIndex,
	}
}

func (r DeleteRangeCall) ToProto() *proto.DeleteRangeRequest {
	return &proto.DeleteRangeRequest{
		StartInclusive: r.MinKeyInclusive,
		EndExclusive:   r.MaxKeyExclusive,
		OpIndex:        r.OpIndex,
	}
}

// InOpIndexOrder returns the calls of a write batch in the order they were
// added to it.
func InOpIndexOrder(puts []PutCall, deletes []DeleteCall, deleteRanges []DeleteRangeCall) []any {
	type indexedCall struct {
		opIndex uint32
		call    any
	}
	calls := make([]indexedCall, 0, len(puts)+len(deletes)+len(deleteRanges))
	for _, put := range puts {
		calls = append(calls, indexedCall{put.OpIndex, put})
	}
	for _, del := range deletes {
		calls = append(calls, indexedCall{del.OpIndex, del})
	}
	for _, deleteRange := range deleteRanges {
		calls = append(calls, indexedCall{deleteRange.OpIndex, deleteRange})
	}
	slices.SortStableFunc(calls, func(a, b indexedCall) int {
		return cmp.Compare(a.opIndex, b.opIndex)
	})

	ordered := make([]any, len(calls))
	for i, c := range calls {
		ordered[i] = c.call
	}
	return ordered
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
