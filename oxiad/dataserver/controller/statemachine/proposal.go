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
	"errors"
	"fmt"
	"time"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/crc"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
)

type Proposal interface {
	GetOffset() int64

	GetTimestamp() uint64

	ToLogEntry(vtEntry *proto.LogEntryValue)

	Apply(db database.DB, callback database.UpdateOperationCallback) (ApplyResponse, error)
}

type ApplyResponse struct {
	WriteResponse *proto.WriteResponse
	Checksum      *crc.Checksum
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

func (wp *WriteProposal) Apply(db database.DB, callback database.UpdateOperationCallback) (ApplyResponse, error) {
	resp, err := db.ProcessWrite(wp.request, wp.offset, wp.timestamp, callback)
	return ApplyResponse{WriteResponse: resp}, err
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

func (c *ControlProposal) Apply(db database.DB, cb database.UpdateOperationCallback) (ApplyResponse, error) {
	meta, err := db.ProcessControlRequest(c.request, c.offset, c.timestamp, cb)
	if err != nil {
		return ApplyResponse{}, err
	}
	return ApplyResponse{
		Checksum: meta.Checksum,
	}, nil
}

func NewControlProposal(offset int64, request *proto.ControlRequest) Proposal {
	return &ControlProposal{
		offset:    offset,
		request:   request,
		timestamp: uint64(time.Now().UnixMilli()),
	}
}

// NewProposalFromLogEntry rebuilds the proposal that a leader appended as the
// log entry: a control request, or a single write request.
func NewProposalFromLogEntry(entry *proto.LogEntry) (Proposal, error) {
	logEntryValue := &proto.LogEntryValue{}
	if err := logEntryValue.UnmarshalVT(entry.Value); err != nil {
		return nil, err
	}

	switch value := logEntryValue.Value.(type) {
	case *proto.LogEntryValue_ControlRequest:
		return &ControlProposal{offset: entry.Offset, timestamp: entry.Timestamp, request: value.ControlRequest}, nil
	case *proto.LogEntryValue_Requests:
		writes := value.Requests.GetWrites()
		if len(writes) != 1 {
			return nil, fmt.Errorf("expected a single write request in the log entry, found %d", len(writes))
		}
		return &WriteProposal{offset: entry.Offset, timestamp: entry.Timestamp, request: writes[0]}, nil
	default:
		return nil, errors.New("unknown proposal type")
	}
}
