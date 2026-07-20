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

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
)

func ApplyLogEntry(db database.DB, entry *proto.LogEntry, updateOperationCallback database.UpdateOperationCallback) (ApplyResponse, error) {
	logEntryValue := proto.LogEntryValueFromVTPool()
	defer logEntryValue.ReturnToVTPool()

	// UnmarshalVTUnsafe aliases every string/bytes field of the decoded tree
	// into entry.Value instead of copying it. The lifetime contract:
	//   - entry.Value is a private heap buffer: the WAL reader materializes
	//     every LogEntry with a copying unmarshal (and the segment codec
	//     itself copies records out of the mmap), so the buffer is never
	//     mutated, recycled, or unmapped behind the aliases.
	//   - Everything ProcessWrite/ProcessControlRequest persists is copied
	//     (Pebble batch arena, sealed notifications) before returning, and
	//     nothing retains the aliased strings past the call.
	//   - The pool only recycles the top-level LogEntryValue: ResetVT nils
	//     the oneof, so aliased sub-messages are never handed out again.
	//   - Pooled StorageEntry objects must not keep request-owned buffers on
	//     pool return (see applyPut), or a later pooled unmarshal would
	//     append over this entry's payload while parts of it are still live.
	if err := logEntryValue.UnmarshalVTUnsafe(entry.Value); err != nil {
		return ApplyResponse{}, err
	}

	switch logEntryValue.Value.(type) {
	case *proto.LogEntryValue_ControlRequest:
		meta, err := db.ProcessControlRequest(logEntryValue.GetControlRequest(), entry.Offset, entry.Timestamp, updateOperationCallback)
		if err != nil {
			return ApplyResponse{}, err
		}
		return ApplyResponse{
			Checksum: meta.Checksum,
		}, nil
	case *proto.LogEntryValue_Requests:
		for _, writeRequest := range logEntryValue.GetRequests().Writes {
			if _, err := db.ProcessWrite(writeRequest, entry.Offset, entry.Timestamp, updateOperationCallback); err != nil {
				return ApplyResponse{}, err
			}
		}
	default:
		return ApplyResponse{}, errors.New("unknown proposal type")
	}
	return ApplyResponse{}, nil
}

// ApplyLogEntryWithSplitFilter applies a log entry with write request filtering
// for a shard split child. Each WriteRequest is filtered to only include
// operations for keys within the child's hash range. If the filtered request
// is empty, the entry still advances the commit offset in the database (via
// ProcessWrite with an empty request) to maintain offset contiguity.
func ApplyLogEntryWithSplitFilter(
	db database.DB,
	entry *proto.LogEntry,
	updateOperationCallback database.UpdateOperationCallback,
	hashRange *proto.HashRange,
) (ApplyResponse, error) {
	logEntryValue := proto.LogEntryValueFromVTPool()
	defer logEntryValue.ReturnToVTPool()

	// Zero-copy decode: same lifetime contract as in ApplyLogEntry above.
	if err := logEntryValue.UnmarshalVTUnsafe(entry.Value); err != nil {
		return ApplyResponse{}, err
	}

	switch logEntryValue.Value.(type) {
	case *proto.LogEntryValue_ControlRequest:
		meta, err := db.ProcessControlRequest(logEntryValue.GetControlRequest(), entry.Offset, entry.Timestamp, updateOperationCallback)
		if err != nil {
			return ApplyResponse{}, err
		}
		return ApplyResponse{
			Checksum: meta.Checksum,
		}, nil
	case *proto.LogEntryValue_Requests:
		for _, writeRequest := range logEntryValue.GetRequests().Writes {
			filtered := database.FilterWriteRequestForSplit(writeRequest, hashRange)
			if filtered == nil {
				// All operations were outside this child's range.
				// Still call ProcessWrite with an empty request to advance commit offset.
				filtered = &proto.WriteRequest{Shard: writeRequest.Shard}
			}
			if _, err := db.ProcessWrite(filtered, entry.Offset, entry.Timestamp, updateOperationCallback); err != nil {
				return ApplyResponse{}, err
			}
		}
	default:
		return ApplyResponse{}, errors.New("unknown proposal type")
	}
	return ApplyResponse{}, nil
}
