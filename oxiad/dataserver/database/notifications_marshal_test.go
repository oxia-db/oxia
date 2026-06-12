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
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
)

func marshalSealed(t *testing.T, n *Notifications) []byte {
	t.Helper()
	nb := n.seal()
	buf := make([]byte, nb.SizeVT())
	written, err := nb.MarshalToSizedBufferVT(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(buf), written)
	return buf
}

// The serialized notification batch feeds the replicated batch checksum, so
// every replica must produce identical bytes for the same logical content —
// including replicas still running the previous code, which marshaled a
// map<string, Notification> field through the protobuf-go Deterministic
// marshaler. The golden vectors below were captured from that code: a
// repeated NotificationEntry field is wire-compatible with the former map,
// and seal() emits the entries in the same ascending key order.
func TestNotificationBatchGoldenBytes(t *testing.T) {
	versionId := int64(7)
	rangeEnd := "range-end"

	golden := []string{
		"",
		"0805102a19d202964900000000",
		"08011009196300000000000000220b0a056b65792d6212021007",
		"0803100b19640000000000000022040a00120022090a016112040801100722160a056d2f6e2f6f120d08031a0972616e67652d656e6422070a017a12020802",
	}

	cases := []*Notifications{
		{byKey: map[string]*proto.Notification{}},
		{
			batch: proto.NotificationBatch{Shard: 5, Offset: 42, Timestamp: 1234567890},
			byKey: map[string]*proto.Notification{},
		},
		{
			batch: proto.NotificationBatch{Shard: 1, Offset: 9, Timestamp: 99},
			byKey: map[string]*proto.Notification{
				"key-b": {Type: proto.NotificationType_KEY_CREATED, VersionId: &versionId},
			},
		},
		{
			batch: proto.NotificationBatch{Shard: 3, Offset: 11, Timestamp: 100},
			byKey: map[string]*proto.Notification{
				"z":     {Type: proto.NotificationType_KEY_DELETED},
				"a":     {Type: proto.NotificationType_KEY_MODIFIED, VersionId: &versionId},
				"m/n/o": {Type: proto.NotificationType_KEY_RANGE_DELETED, KeyRangeLast: &rangeEnd},
				"":      {Type: proto.NotificationType_KEY_CREATED},
			},
		},
	}

	// Many keys, inserted in reverse order: the probability that an unsorted
	// iteration accidentally matches the golden bytes is negligible
	many := &Notifications{
		batch: proto.NotificationBatch{Shard: 2, Offset: 1, Timestamp: 1},
		byKey: map[string]*proto.Notification{},
	}
	for i := 0; i < 30; i++ {
		v := int64(i)
		many.byKey[fmt.Sprintf("key-%04d", 9999-i)] =
			&proto.Notification{Type: proto.NotificationType_KEY_MODIFIED, VersionId: &v}
	}
	cases = append(cases, many)
	golden = append(golden, "0802100119010000000000000022100a086b65792d3939373012040801101d22100a086b65792d3939373112040801101c22100a086b65792d3939373212040801101b22100a086b65792d3939373312040801101a22100a086b65792d3939373412040801101922100a086b65792d3939373512040801101822100a086b65792d3939373612040801101722100a086b65792d3939373712040801101622100a086b65792d3939373812040801101522100a086b65792d3939373912040801101422100a086b65792d3939383012040801101322100a086b65792d3939383112040801101222100a086b65792d3939383212040801101122100a086b65792d3939383312040801101022100a086b65792d3939383412040801100f22100a086b65792d3939383512040801100e22100a086b65792d3939383612040801100d22100a086b65792d3939383712040801100c22100a086b65792d3939383812040801100b22100a086b65792d3939383912040801100a22100a086b65792d3939393012040801100922100a086b65792d3939393112040801100822100a086b65792d3939393212040801100722100a086b65792d3939393312040801100622100a086b65792d3939393412040801100522100a086b65792d3939393512040801100422100a086b65792d3939393612040801100322100a086b65792d3939393712040801100222100a086b65792d3939393812040801100122100a086b65792d39393939120408011000")

	for i, n := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			expected, err := hex.DecodeString(golden[i])
			assert.NoError(t, err)

			actual := marshalSealed(t, n)
			assert.Equal(t, expected, actual)

			// Stable across repeated seals and marshals
			for range 20 {
				assert.Equal(t, expected, marshalSealed(t, n))
			}

			// Round-trips with the entries intact
			decoded := &proto.NotificationBatch{}
			assert.NoError(t, decoded.UnmarshalVT(actual))
			assert.Equal(t, len(n.byKey), len(decoded.Notifications))
			for _, entry := range decoded.Notifications {
				expectedNotification, found := n.byKey[entry.GetKey()]
				assert.True(t, found)
				assert.True(t, pb.Equal(expectedNotification, entry.Value))
			}
		})
	}
}

func BenchmarkNotificationBatchMarshal(b *testing.B) {
	versionId := int64(7)
	notifications := &Notifications{
		batch: proto.NotificationBatch{Shard: 2, Offset: 1, Timestamp: 1234567890},
		byKey: map[string]*proto.Notification{},
	}
	for i := 0; i < 10; i++ {
		notifications.byKey[fmt.Sprintf("/app/users/%04d/profile", i)] =
			&proto.Notification{Type: proto.NotificationType_KEY_MODIFIED, VersionId: &versionId}
	}

	b.Run("reflection-deterministic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			nb := notifications.seal()
			if _, err := (pb.MarshalOptions{Deterministic: true}).Marshal(nb); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("vtproto", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			nb := notifications.seal()
			buf := make([]byte, nb.SizeVT())
			if _, err := nb.MarshalToSizedBufferVT(buf); err != nil {
				b.Fatal(err)
			}
		}
	})
}
