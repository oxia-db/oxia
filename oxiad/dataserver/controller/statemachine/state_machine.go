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

	if err := logEntryValue.UnmarshalVT(entry.Value); err != nil {
		return ApplyResponse{}, err
	}

	switch logEntryValue.Value.(type) {
	case *proto.LogEntryValue_ControlRequest:
		switch v := logEntryValue.GetControlRequest().Value.(type) {
		case *proto.ControlRequest_FeatureEnable:
			for _, feature := range v.FeatureEnable.GetFeatures() {
				db.EnableFeature(feature)
			}
		case *proto.ControlRequest_RecordChecksum:
			checksum := db.ReadChecksum()
			return ApplyResponse{Checksum: &checksum}, nil
		default:
			return ApplyResponse{}, errors.New("unknown control request type")
		}
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

	if err := logEntryValue.UnmarshalVT(entry.Value); err != nil {
		return ApplyResponse{}, err
	}

	switch logEntryValue.Value.(type) {
	case *proto.LogEntryValue_ControlRequest:
		switch v := logEntryValue.GetControlRequest().Value.(type) {
		case *proto.ControlRequest_FeatureEnable:
			for _, feature := range v.FeatureEnable.GetFeatures() {
				db.EnableFeature(feature)
			}
		case *proto.ControlRequest_RecordChecksum:
			checksum := db.ReadChecksum()
			return ApplyResponse{Checksum: &checksum}, nil
		default:
			// Unknown control request type — ignore
		}
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
