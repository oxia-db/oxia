// Copyright 2023-2025 The Oxia Authors
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

package bench

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/node/constant"
	nodewal "github.com/oxia-db/oxia/node/wal"

	"github.com/oxia-db/oxia/proto"
)

func Benchmark_Wal_Append(b *testing.B) {
	walFactory := nodewal.NewWalFactory(&nodewal.FactoryOptions{
		BaseWalDir:  b.TempDir(),
		Retention:   time.Minute,
		SegmentSize: 1024 * 1024,
		SyncData:    true,
	})
	wal, err := walFactory.NewWal("test", 1, nil)
	assert.NoError(b, err)
	defer wal.Close()

	n := time.Now()
	for i := 0; i < b.N; i++ {
		err := wal.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte("value"),
		})
		assert.NoError(b, err)
	}
	fmt.Printf("%s Count(%d) Latency(Per Entry): %d\n", b.Name(), b.N, time.Since(n).Microseconds()/int64(b.N))
}

func Benchmark_Wal_Append_with_Read(b *testing.B) {
	walFactory := nodewal.NewWalFactory(&nodewal.FactoryOptions{
		BaseWalDir:  b.TempDir(),
		Retention:   time.Minute,
		SegmentSize: 1024 * 1024,
		SyncData:    true,
	})
	wal, err := walFactory.NewWal("test", 1, nil)
	assert.NoError(b, err)
	defer wal.Close()

	wg := sync.WaitGroup{}
	c := 2
	n := time.Now()
	wg.Go(func() {
		for i := 0; i < b.N; i++ {
			err := wal.Append(&proto.LogEntry{
				Term:   1,
				Offset: int64(i),
				Value:  []byte("value"),
			})
			assert.NoError(b, err)
		}
	})

	for i := 0; i < c-1; i++ {
		wg.Go(func() {
			reader, err := wal.NewReader(constant.InvalidOffset)
			assert.NoError(b, err)

			for i := 0; i < b.N; i++ {
				for {
					if !reader.HasNext() {
						continue
					}

					_, err := reader.ReadNext()
					assert.NoError(b, err)
					break
				}
			}
		})
	}

	wg.Wait()
	fmt.Printf("%s Count(%d) Latency(Per Entry): %d\n", b.Name(), b.N, time.Since(n).Microseconds()/int64(b.N))
}

func Benchmark_Wal_Append_to_Read_latency(b *testing.B) {
	walFactory := nodewal.NewWalFactory(&nodewal.FactoryOptions{
		BaseWalDir:  b.TempDir(),
		Retention:   time.Minute,
		SegmentSize: 1024 * 1024,
		SyncData:    true,
	})
	wal, err := walFactory.NewWal("test", 1, nil)
	assert.NoError(b, err)
	defer wal.Close()
	diffs := make([]int64, b.N)

	wg := sync.WaitGroup{}
	wg.Go(func() {
		for i := 0; i < b.N; i++ {
			ts := time.Now().UnixMicro()

			err := wal.Append(&proto.LogEntry{
				Term:      1,
				Offset:    int64(i),
				Value:     []byte{},
				Timestamp: uint64(ts),
			})
			assert.NoError(b, err)
		}
	})

	wg.Go(func() {
		reader, err := wal.NewReader(constant.InvalidOffset)
		assert.NoError(b, err)
		for i := 0; i < b.N; i++ {
			for {
				if !reader.HasNext() {
					continue
				}

				entry, err := reader.ReadNext()
				assert.NoError(b, err)
				diff := time.Now().UnixMicro() - int64(entry.Timestamp)
				diffs[i] = diff
				break
			}
		}
	})

	wg.Wait()

	type Bucket struct {
		Count int
		Value int64
	}
	latency := []Bucket{
		{Count: 0, Value: 10},            // <10
		{Count: 0, Value: 20},            // 10<= & <20
		{Count: 0, Value: 30},            // 20<= & <30
		{Count: 0, Value: 40},            // 30<= & <40
		{Count: 0, Value: 50},            // 40<= & <50
		{Count: 0, Value: 60},            // 50<= & <60
		{Count: 0, Value: 70},            // 60<= & <70
		{Count: 0, Value: math.MaxInt64}, // 70<=
	}
	for _, diff := range diffs {
		if diff < 0 {
			panic("Should not happen when observe an negative diff")
		}

		for i := range latency {
			if diff < latency[i].Value {
				latency[i].Count++
				break
			}
		}
	}
	fmt.Printf(`
%s - Latency(MicroSecond):
    <10: %d
    <20: %d
    <30: %d
    <40: %d
    <50: %d
    <60: %d
    <70: %d
    <If: %d
	`, b.Name(),
		latency[0].Count,
		latency[1].Count,
		latency[2].Count,
		latency[3].Count,
		latency[4].Count,
		latency[5].Count,
		latency[6].Count,
		latency[7].Count,
	)
}
