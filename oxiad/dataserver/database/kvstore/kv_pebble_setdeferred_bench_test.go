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

package kvstore

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/oxia-db/oxia/common/proto"
)

// Isolates pebble Set (marshal to a fresh buffer, then copy into the batch)
// against SetDeferred (marshal directly into the batch arena) on an in-memory
// FS, mirroring the applyPut sequence: indexed batch, version-check Get, put,
// commit. This is the difference between WriteBatch.Put with a pre-marshaled
// buffer and WriteBatch.PutMarshalable.
func BenchmarkPebbleSetVsSetDeferred(b *testing.B) {
	for _, valueSize := range []int{128, 4096, 65536} {
		value := bytes.Repeat([]byte("x"), valueSize)
		sessionId := int64(42)
		se := &proto.StorageEntry{
			Value:                 value,
			VersionId:             7,
			ModificationsCount:    3,
			CreationTimestamp:     100,
			ModificationTimestamp: 200,
			SessionId:             &sessionId,
		}
		key := []byte("bench-key")

		runOne := func(b *testing.B, deferred bool) {
			b.Helper()
			db, err := pebble.Open("", &pebble.Options{
				FS:         vfs.NewMem(),
				DisableWAL: true,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := db.NewIndexedBatch()

				// version-check read, as applyPut does
				if v, closer, err := batch.Get(key); err == nil {
					_ = v
					_ = closer.Close()
				} else if !errors.Is(err, pebble.ErrNotFound) {
					b.Fatal(err)
				}

				if deferred {
					op := batch.SetDeferred(len(key), se.SizeVT())
					copy(op.Key, key)
					if _, err := se.MarshalToSizedBufferVT(op.Value); err != nil {
						b.Fatal(err)
					}
					if err := op.Finish(); err != nil {
						b.Fatal(err)
					}
				} else {
					ser, err := se.MarshalVT()
					if err != nil {
						b.Fatal(err)
					}
					if err := batch.Set(key, ser, pebble.NoSync); err != nil {
						b.Fatal(err)
					}
				}

				if err := batch.Commit(pebble.NoSync); err != nil {
					b.Fatal(err)
				}
				if err := batch.Close(); err != nil {
					b.Fatal(err)
				}
			}
		}

		b.Run(fmt.Sprintf("set/value-%d", valueSize), func(b *testing.B) { runOne(b, false) })
		b.Run(fmt.Sprintf("setDeferred/value-%d", valueSize), func(b *testing.B) { runOne(b, true) })
	}
}
