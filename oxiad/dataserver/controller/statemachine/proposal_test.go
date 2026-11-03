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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
)

func TestNewWriteProposal(t *testing.T) {
	request := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{Key: "a", Value: []byte("v")}},
	}

	proposal := NewWriteProposal(5, 0, request)

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

	proposal := NewControlProposal(10, 0, request)

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

	proposal := NewWriteProposal(1, 0, request)

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

	proposal := NewControlProposal(2, 0, request)

	entryValue := &proto.LogEntryValue{}
	proposal.ToLogEntry(entryValue)

	controlReq, ok := entryValue.Value.(*proto.LogEntryValue_ControlRequest)
	assert.True(t, ok)
	featureEnable := controlReq.ControlRequest.GetFeatureEnable()
	assert.NotNil(t, featureEnable)
	assert.Equal(t, 1, len(featureEnable.Features))
	assert.Equal(t, proto.Feature_FEATURE_DB_CHECKSUM, featureEnable.Features[0])
}

func TestControlProposalApplyReturnsDBError(t *testing.T) {
	db := newTestDB(t)

	proposal := NewControlProposal(0, 0, &proto.ControlRequest{
		Value: &proto.ControlRequest_FeatureEnable{
			FeatureEnable: &proto.FeatureEnableRequest{Features: []proto.Feature{proto.Feature(math.MaxInt32)}},
		},
	})

	_, err := proposal.Apply(db, database.NoOpCallback)
	assert.ErrorIs(t, err, constant.ErrUnsupportedFeatures)
}

func TestNewProposalFromLogEntry(t *testing.T) {
	for _, proposal := range []Proposal{
		NewWriteProposal(3, 0, &proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "a", Value: []byte("v")}}}),
		NewControlProposal(4, 0, &proto.ControlRequest{
			Value: &proto.ControlRequest_RecordChecksum{RecordChecksum: &proto.RecordChecksumRequest{}},
		}),
	} {
		entryValue := &proto.LogEntryValue{}
		proposal.ToLogEntry(entryValue)
		value, err := entryValue.MarshalVT()
		require.NoError(t, err)

		rebuilt, err := NewProposalFromLogEntry(&proto.LogEntry{
			Offset:    proposal.GetOffset(),
			Timestamp: proposal.GetTimestamp(),
			Value:     value,
		})
		require.NoError(t, err)
		assert.IsType(t, proposal, rebuilt)
		assert.Equal(t, proposal.GetOffset(), rebuilt.GetOffset())
		assert.Equal(t, proposal.GetTimestamp(), rebuilt.GetTimestamp())
		rebuiltValue := &proto.LogEntryValue{}
		rebuilt.ToLogEntry(rebuiltValue)
		assert.True(t, pb.Equal(entryValue, rebuiltValue))
	}

	// A leader appends a single write request per entry
	value, err := (&proto.LogEntryValue{Value: &proto.LogEntryValue_Requests{Requests: &proto.WriteRequests{
		Writes: []*proto.WriteRequest{{}, {}},
	}}}).MarshalVT()
	require.NoError(t, err)
	_, err = NewProposalFromLogEntry(&proto.LogEntry{Value: value})
	assert.Error(t, err)
}

func TestNewProposalFromLogEntryInvalidBytes(t *testing.T) {
	_, err := NewProposalFromLogEntry(&proto.LogEntry{Value: []byte("invalid-protobuf-bytes")})
	assert.Error(t, err)
}

func TestNewProposalFromLogEntryUnknownType(t *testing.T) {
	// An entry value without any request
	_, err := NewProposalFromLogEntry(&proto.LogEntry{Value: []byte{}})
	assert.ErrorContains(t, err, "unknown proposal type")
}
