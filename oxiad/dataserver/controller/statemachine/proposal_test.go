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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestNewWriteProposal(t *testing.T) {
	request := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{Key: "a", Value: []byte("v")}},
	}

	proposal := NewWriteProposal(5, request)

	assert.EqualValues(t, 5, proposal.GetOffset())
	assert.NotZero(t, proposal.GetTimestamp())
	assert.IsType(t, &WriteProposal{}, proposal)
}

func TestNewControlProposal(t *testing.T) {
	request := &proto.ControlRequest{
		Value: &proto.ControlRequest_FeatureEnable{
			FeatureEnable: &proto.FeatureEnableRequest{
				Features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM},
			},
		},
	}

	proposal := NewControlProposal(10, request)

	assert.EqualValues(t, 10, proposal.GetOffset())
	assert.NotZero(t, proposal.GetTimestamp())
	assert.IsType(t, &ControlProposal{}, proposal)
}

func TestWriteProposal_ToLogEntry(t *testing.T) {
	request := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "key1", Value: []byte("val1")},
			{Key: "key2", Value: []byte("val2")},
		},
	}

	proposal := NewWriteProposal(1, request)

	entryValue := &proto.LogEntryValue{}
	proposal.ToLogEntry(entryValue)

	requests, ok := entryValue.Value.(*proto.LogEntryValue_Requests)
	assert.True(t, ok)
	assert.Equal(t, 1, len(requests.Requests.Writes))
	assert.Equal(t, 2, len(requests.Requests.Writes[0].Puts))
	assert.Equal(t, "key1", requests.Requests.Writes[0].Puts[0].Key)
	assert.Equal(t, "key2", requests.Requests.Writes[0].Puts[1].Key)
}

func TestControlProposal_ToLogEntry(t *testing.T) {
	request := &proto.ControlRequest{
		Value: &proto.ControlRequest_FeatureEnable{
			FeatureEnable: &proto.FeatureEnableRequest{
				Features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM},
			},
		},
	}

	proposal := NewControlProposal(2, request)

	entryValue := &proto.LogEntryValue{}
	proposal.ToLogEntry(entryValue)

	controlReq, ok := entryValue.Value.(*proto.LogEntryValue_ControlRequest)
	assert.True(t, ok)
	featureEnable := controlReq.ControlRequest.GetFeatureEnable()
	assert.NotNil(t, featureEnable)
	assert.Equal(t, 1, len(featureEnable.Features))
	assert.Equal(t, proto.Feature_FEATURE_DB_CHECKSUM, featureEnable.Features[0])
}
