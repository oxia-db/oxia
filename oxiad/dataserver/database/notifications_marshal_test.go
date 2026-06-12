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

package database

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
)

func marshalDeterministicVT(t *testing.T, nb *proto.NotificationBatch) []byte {
	t.Helper()
	d := sortedNotificationBatch(nb)
	buf := make([]byte, d.SizeVT())
	n, err := d.MarshalToSizedBufferVT(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(buf), n)
	return buf
}

// The notification value feeds the replicated batch checksum: the sorted twin
// message must marshal to exactly the bytes of the protobuf-go Deterministic
// marshaler it replaced, or replicas on different versions would disagree on
// the checksum. This also acts as a canary if NotificationBatch ever gains
// fields that SortedNotificationBatch does not mirror.
func TestSortedNotificationBatchMatchesStdlib(t *testing.T) {
	versionId := int64(7)
	rangeEnd := "range-end"

	cases := []*proto.NotificationBatch{
		{},
		{Shard: 5, Offset: 42, Timestamp: 1234567890},
		{
			Shard:     1,
			Offset:    9,
			Timestamp: 99,
			Notifications: map[string]*proto.Notification{
				"key-b": {Type: proto.NotificationType_KEY_CREATED, VersionId: &versionId},
			},
		},
		{
			Shard:     3,
			Offset:    11,
			Timestamp: 100,
			Notifications: map[string]*proto.Notification{
				"z":     {Type: proto.NotificationType_KEY_DELETED},
				"a":     {Type: proto.NotificationType_KEY_MODIFIED, VersionId: &versionId},
				"m/n/o": {Type: proto.NotificationType_KEY_RANGE_DELETED, KeyRangeLast: &rangeEnd},
				"":      {Type: proto.NotificationType_KEY_CREATED},
			},
		},
	}

	// Many keys: the probability that an unsorted map iteration accidentally
	// matches the sorted order is negligible
	many := &proto.NotificationBatch{Shard: 2, Offset: 1, Timestamp: 1,
		Notifications: map[string]*proto.Notification{}}
	for i := 0; i < 30; i++ {
		v := int64(i)
		many.Notifications[fmt.Sprintf("key-%04d", 9999-i)] =
			&proto.Notification{Type: proto.NotificationType_KEY_MODIFIED, VersionId: &v}
	}
	cases = append(cases, many)

	for i, nb := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			expected, err := pb.MarshalOptions{Deterministic: true}.Marshal(nb)
			assert.NoError(t, err)

			actual := marshalDeterministicVT(t, nb)
			assert.Equal(t, expected, actual)

			// Stable across repeated marshals
			for range 20 {
				assert.Equal(t, actual, marshalDeterministicVT(t, nb))
			}

			// And it round-trips
			decoded := &proto.NotificationBatch{}
			assert.NoError(t, decoded.UnmarshalVT(actual))
			assert.True(t, pb.Equal(nb, decoded))
		})
	}
}

func BenchmarkNotificationBatchMarshal(b *testing.B) {
	versionId := int64(7)
	nb := &proto.NotificationBatch{Shard: 2, Offset: 1, Timestamp: 1234567890,
		Notifications: map[string]*proto.Notification{}}
	for i := 0; i < 10; i++ {
		nb.Notifications[fmt.Sprintf("/app/users/%04d/profile", i)] =
			&proto.Notification{Type: proto.NotificationType_KEY_MODIFIED, VersionId: &versionId}
	}

	b.Run("reflection-deterministic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := (pb.MarshalOptions{Deterministic: true}).Marshal(nb); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("vtproto-sorted-twin", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			d := sortedNotificationBatch(nb)
			buf := make([]byte, d.SizeVT())
			if _, err := d.MarshalToSizedBufferVT(buf); err != nil {
				b.Fatal(err)
			}
		}
	})
}
