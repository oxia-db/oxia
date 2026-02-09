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

type ApplyResponse struct {
	WriteResponse *proto.WriteResponse
}

func ApplyProposal(db database.DB, proposal Proposal, updateOperationCallback database.UpdateOperationCallback) (ApplyResponse, error) {
	response := ApplyResponse{}
	var err error
	switch p := proposal.(type) {
	case *ControlProposal:
		applyControlRequest(db, p.request)
	case *WriteProposal:
		response.WriteResponse, err = applyWriteRequest(db, p.request, p.offset, p.timestamp, updateOperationCallback)
	default:
		return response, errors.New("unknown proposal type")
	}
	return response, err
}

func ApplyLogEntry(db database.DB, entry *proto.LogEntry, updateOperationCallback database.UpdateOperationCallback) error {
	logEntryValue := proto.LogEntryValueFromVTPool()
	defer logEntryValue.ReturnToVTPool()

	if err := logEntryValue.UnmarshalVT(entry.Value); err != nil {
		return err
	}

	switch logEntryValue.Value.(type) {
	case *proto.LogEntryValue_ControlRequest:
		applyControlRequest(db, logEntryValue.GetControlRequest())
	case *proto.LogEntryValue_Requests:
		for _, writeRequest := range logEntryValue.GetRequests().Writes {
			if _, err := applyWriteRequest(db, writeRequest, entry.Offset, entry.Timestamp, updateOperationCallback); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyControlRequest(db database.DB, request *proto.ControlRequest) {
	if featureEnable := request.GetFeatureEnable(); featureEnable != nil {
		for _, feature := range featureEnable.GetFeatures() {
			db.EnableFeature(feature)
		}
	}
}

func applyWriteRequest(db database.DB, request *proto.WriteRequest, offset int64, timestamp uint64,
	updateOperationCallback database.UpdateOperationCallback) (*proto.WriteResponse, error) {
	return db.ProcessWrite(request, offset, timestamp, updateOperationCallback)
}
