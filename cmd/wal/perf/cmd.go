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

package perf

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/wal/common"
	"github.com/oxia-db/oxia/proto"
	"github.com/oxia-db/oxia/server/wal"
)

type perfOptions struct {
	entryCount      int64
	entrySize       int64
	readerCount     int64
	segmentSize     int64
	syncData        bool
	retentionSecond int64
}

var (
	options = perfOptions{}
	Cmd     = &cobra.Command{
		Use:   "perf",
		Short: "Performance test",
		RunE:  run,
	}
)

func init() {
	Cmd.Flags().Int64Var(&options.entryCount, "entry-count", 1000000, "number of entries to write")
	Cmd.Flags().Int64Var(&options.entrySize, "entry-size", 100, "size of each entry")
	Cmd.Flags().Int64Var(&options.readerCount, "reader-count", 1, "number of readers")
	Cmd.Flags().Int64Var(&options.segmentSize, "segment-size", 1024*1024, "size of each segment")
	Cmd.Flags().BoolVar(&options.syncData, "sync-data", true, "whether to sync data after each write")
	Cmd.Flags().Int64Var(&options.retentionSecond, "retention-second", 3600, "retention time for the wal")
}

//nolint:revive
func run(*cobra.Command, []string) error {
	//
	//process.PprofEnable = true
	//process.PprofBindAddress = "127.0.0.1:6060"
	//process.RunProfiling()

	if err := os.RemoveAll(common.WalOption.WalDir); err != nil {
		panic(err)
	}
	factory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  common.WalOption.WalDir,
		Retention:   math.MaxInt64,
		SegmentSize: int32(options.segmentSize),
		SyncData:    options.syncData,
	})
	writeAheadLog, err := factory.NewWal(common.WalOption.Namespace, common.WalOption.Shard, nil)
	if err != nil {
		panic(err)
	}
	defer writeAheadLog.Close()

	threadTimesArray := make([]int64, options.readerCount+1)
	writeEntryTimesArray := make([]int64, options.entryCount)
	readEntryTimesArray := make([]int64, (options.readerCount)*options.entryCount)
	data := make([]byte, options.entrySize)
	for i := 0; i < len(data); i++ {
		data[i] = 'a'
	}

	var wg sync.WaitGroup
	wg.Add(1 + int(options.readerCount))
	go func() {
		defer wg.Done()
		n := time.Now().UnixMicro()

		writeGroup := sync.WaitGroup{}
		for i := int64(0); i < options.entryCount; i++ {
			writeGroup.Add(1)
			entry := &proto.LogEntry{
				Term:      1,
				Offset:    i,
				Value:     data,
				Timestamp: uint64(time.Now().UnixMicro()),
			}
			writeAheadLog.AppendAndSync(entry, func(err error) {
				writeEntryTimesArray[i] = time.Now().UnixMicro() - int64(entry.Timestamp)
				writeGroup.Done()
				if err != nil {
					panic(err)
				}
			})
		}
		writeGroup.Wait()
		threadTimesArray[0] = time.Now().UnixMicro() - n
		slog.Info("writer thread has done", slog.Int64("Elapse", threadTimesArray[0]))
	}()

	for i := int64(0); i < options.readerCount; i++ {
		go func(readerIdx int64) {
			defer wg.Done()
			n := time.Now().UnixMicro()

			reader, err := writeAheadLog.NewReader(wal.InvalidOffset)
			if err != nil {
				panic(err)
			}

			for i := int64(0); i < options.entryCount; i++ {
				for {
					if !reader.HasNext() {
						continue
					}

					entry, err := reader.ReadNext()
					if err != nil {
						panic(err)
					}
					readEntryTimesArray[readerIdx*options.entryCount+i] = time.Now().UnixMicro() - int64(entry.Timestamp)
					break
				}
			}

			threadTimesArray[readerIdx+1] = time.Now().UnixMicro() - n
			slog.Info("reader thread has done",
				slog.Int64("Elapse", threadTimesArray[readerIdx+1]),
				slog.Int64("ReaderIdx", readerIdx),
			)
		}(i)
	}

	wg.Wait()
	for i := 0; i < len(threadTimesArray); i++ {
		threadName := "Writer"
		if i > 0 {
			threadName = fmt.Sprintf("Reader%d", i-1)
		}

		//nolint:revive
		fmt.Printf("Thread: %s, Total entry: %d, Total time: %s, Throughput: %f ops/s\n",
			threadName,
			options.entryCount,
			time.Duration(threadTimesArray[i])*time.Microsecond,
			float64(options.entryCount)/float64(threadTimesArray[i])*1000000)
	}

	printLatench("Writer", writeEntryTimesArray)
	printLatench("Reader", readEntryTimesArray)
	return nil
}

func printLatench(name string, datas []int64) {
	summary := prometheus.NewSummary(prometheus.SummaryOpts{
		Name: name,
		Help: "",
		Objectives: map[float64]float64{
			0.5:   0.05,
			0.9:   0.01,
			0.99:  0.001,
			0.999: 0.0001,
		},
	})
	gather := prometheus.NewRegistry()
	gather.MustRegister(summary)
	for _, t := range datas {
		summary.Observe(float64(t))
	}
	metrics, err := gather.Gather()
	if err != nil {
		panic(err)
	}
	p50, p90, p99, p999 := float64(0), float64(0), float64(0), float64(0)
	for _, metric := range metrics {
		for _, s := range metric.Metric[0].Summary.Quantile {
			v := int64(*s.Quantile * 1000)
			switch v {
			case 500:
				p50 = *s.Value
			case 900:
				p90 = *s.Value
			case 990:
				p99 = *s.Value
			case 999:
				p999 = *s.Value
			default:
				panic("unknown summary quantile")
			}
		}
	}

	fmt.Printf(`
Histogram of %s latency:
P50:  %s
P90:  %s 
P99:  %s 
P999: %s
`, name,
		time.Duration(p50)*time.Microsecond,
		time.Duration(p90)*time.Microsecond,
		time.Duration(p99)*time.Microsecond,
		time.Duration(p999)*time.Microsecond,
	)
}
