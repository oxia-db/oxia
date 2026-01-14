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

package dataserver

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/oxia-db/oxia/cmd/perf"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
)

func BenchmarkServer(b *testing.B) {
	logging.LogLevel = slog.LevelInfo
	logging.ConfigureLogger()

	tmp := b.TempDir()

	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = constant.LocalhostAnyPort
	options.Server.Internal.BindAddress = constant.LocalhostAnyPort
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Storage.Database.Dir = fmt.Sprintf("%s/db", tmp)
	options.Storage.WAL.Dir = fmt.Sprintf("%s/wal", tmp)

	standaloneConf := StandaloneConfig{
		NumShards:         1,
		DataServerOptions: *options,
	}

	standalone, err := NewStandalone(standaloneConf)
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
