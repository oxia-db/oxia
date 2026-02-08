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
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
)

func ApplyLogEntry(db database.DB, entry *proto.LogEntry, updateOperationCallback database.UpdateOperationCallback) error {
	logEntryValue := proto.LogEntryValueFromVTPool()
	defer logEntryValue.ReturnToVTPool()

	if err := pb.Unmarshal(entry.Value, logEntryValue); err != nil {
		return err
	}

	switch logEntryValue.Value.(type) {
	case *proto.LogEntryValue_ControlRequest:
		if request := logEntryValue.GetControlRequest(); request != nil {
			if featureEnabled := request.GetFeatureEnable(); featureEnabled != nil {
				if features := featureEnabled.GetFeatures(); features != nil {
					for _, feature := range features {
						db.EnableFeature(feature)
					}
				}
			}
		}
	case *proto.LogEntryValue_Requests:
		for _, writeRequest := range logEntryValue.GetRequests().Writes {
			if _, err := db.ProcessWrite(writeRequest, entry.Offset, entry.Timestamp, updateOperationCallback); err != nil {
				return err
			}
		}
	}
	return nil
}
