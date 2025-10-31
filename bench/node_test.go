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
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/oxia-db/oxia/common/logging"
	"github.com/oxia-db/oxia/node"
	"github.com/oxia-db/oxia/node/conf"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/perf"
)

func BenchmarkNode(b *testing.B) {
	logging.LogLevel = slog.LevelInfo
	logging.ConfigureLogger()

	tmp := b.TempDir()

	standaloneConf := node.StandaloneConfig{
		NumShards: 1,
		Config: conf.Config{
			InternalServiceAddr: "localhost:0",
			PublicServiceAddr:   "localhost:0",
			MetricsServiceAddr:  "localhost:0",
			DataDir:             fmt.Sprintf("%s/db", tmp),
			WalDir:              fmt.Sprintf("%s/wal", tmp),
		},
	}

	standalone, err := node.NewStandalone(standaloneConf)
	if err != nil {
		b.Fatal(err)
	}

	perfConf := perf.Config{
		Namespace:           "default",
		ServiceAddr:         standalone.ServiceAddr(),
		RequestRate:         200_000,
		ReadPercentage:      80,
		KeysCardinality:     10_000,
		ValueSize:           1_024,
		BatchLinger:         oxia.DefaultBatchLinger,
		MaxRequestsPerBatch: oxia.DefaultMaxRequestsPerBatch,
		RequestTimeout:      oxia.DefaultRequestTimeout,
	}

	profile := "profile-zstd"
	file, err := os.Create(profile)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = file.Close()
	}()

	err = pprof.StartCPUProfile(file)
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Minute))
	defer cancel()

	perf.New(perfConf).Run(ctx)

	pprof.StopCPUProfile()

	err = standalone.Close()
	if err != nil {
		b.Fatal(err)
	}

	var out []byte
	out, err = exec.Command("go", "tool", "pprof", "-http=:", profile).CombinedOutput()
	if err != nil {
		b.Fatal(string(out), err)
	}
	fmt.Println(string(out))
}
