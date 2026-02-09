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

package statemachine

import (
	"time"

	"github.com/oxia-db/oxia/common/proto"
)

type Proposal interface {
	GetOffset() int64

	GetTimestamp() uint64

	ToLogEntry(vtEntry *proto.LogEntryValue)
}

var _ Proposal = &WriteProposal{}

type WriteProposal struct {
	offset    int64
	timestamp uint64
	request   *proto.WriteRequest
}

func (wp *WriteProposal) GetTimestamp() uint64 {
	return wp.timestamp
}

func (wp *WriteProposal) GetOffset() int64 {
	return wp.offset
}

func (wp *WriteProposal) ToLogEntry(vtEntry *proto.LogEntryValue) {
	vtEntry.Value = &proto.LogEntryValue_Requests{Requests: &proto.WriteRequests{Writes: []*proto.WriteRequest{wp.request}}}
}

func NewWriteProposal(offset int64, request *proto.WriteRequest) Proposal {
	return &WriteProposal{
		offset:    offset,
		request:   request,
		timestamp: uint64(time.Now().UnixMilli()),
	}
}

var _ Proposal = &ControlProposal{}

type ControlProposal struct {
	offset    int64
	timestamp uint64
	request   *proto.ControlRequest
}

func (c *ControlProposal) GetOffset() int64 {
	return c.offset
}

func (c *ControlProposal) GetTimestamp() uint64 {
	return c.timestamp
}

func (c *ControlProposal) ToLogEntry(vtEntry *proto.LogEntryValue) {
	vtEntry.Value = &proto.LogEntryValue_ControlRequest{ControlRequest: c.request}
}

func NewControlProposal(offset int64, request *proto.ControlRequest) Proposal {
	return &ControlProposal{
		offset:    offset,
		request:   request,
		timestamp: uint64(time.Now().UnixMilli()),
	}
}
