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
	"context"
	"fmt"
	"reflect"
	"testing"
	stdtime "time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

func TestNotificationsTrimmer_GapFromControlRequests(t *testing.T) {
	clock := &oxiatime.MockedClock{}

	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	defer factory.Close()

	db, err := database.NewDB(
		constant.DefaultNamespace, 1, factory,
		proto.KeySortingType_NATURAL,
		10*stdtime.Millisecond, clock,
	)
	assert.NoError(t, err)
	defer db.Close()

	controlOffsets := map[int64]bool{5: true, 12: true, 18: true}
	for offset := int64(0); offset < 25; offset++ {
		var proposal Proposal
		if controlOffsets[offset] {
			proposal = NewControlProposal(offset, &proto.ControlRequest{
				Value: &proto.ControlRequest_RecordChecksum{
					RecordChecksum: &proto.RecordChecksumRequest{},
				},
			})
		} else {
			proposal = NewWriteProposal(offset, &proto.WriteRequest{
				Puts: []*proto.PutRequest{{
					Key:   fmt.Sprintf("key-%d", offset),
					Value: []byte("v"),
				}},
			})
		}

		entryValue := &proto.LogEntryValue{}
		proposal.ToLogEntry(entryValue)
		value, err := entryValue.MarshalVT()
		assert.NoError(t, err)

		entry := &proto.LogEntry{
			Term:      1,
			Offset:    offset,
			Value:     value,
			Timestamp: uint64(offset),
		}
		_, err = ApplyLogEntry(db, entry, database.NoOpCallback)
		assert.NoError(t, err)
	}

	clock.Set(30)
	assert.Eventually(t, func() bool {
		nb, err := db.ReadNextNotifications(context.Background(), 0)
		if err != nil {
			return false
		}
		got := make([]int64, 0, len(nb))
		for _, b := range nb {
			got = append(got, b.Offset)
		}
		return reflect.DeepEqual(got, []int64{21, 22, 23, 24})
	}, 10*stdtime.Second, 500*stdtime.Millisecond)

	clock.Set(100)
	assert.Eventually(t, func() bool {
		nb, err := db.ReadNextNotifications(context.Background(), 0)
		if err != nil {
			return false
		}
		return len(nb) == 0
	}, 10*stdtime.Second, 500*stdtime.Millisecond)
}
