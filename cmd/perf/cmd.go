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
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"

	"github.com/oxia-db/oxia/oxia"
)

var (
	Cmd = &cobra.Command{
		Use:   "perf",
		Short: "Oxia perf client",
		Long:  `Oxia tool for basic performance tests`,
		Run:   exec,
	}

	config = Config{}
)

func init() {
	defaultServiceAddress := fmt.Sprintf("localhost:%d", constant.DefaultPublicPort)
	Cmd.Flags().StringVarP(&config.ServiceAddr, "service-address", "a", defaultServiceAddress, "Service address")
	Cmd.PersistentFlags().StringVarP(&config.Namespace, "namespace", "n", oxia.DefaultNamespace, "The Oxia namespace to use")

	Cmd.Flags().Float64VarP(&config.RequestRate, "rate", "r", 100.0, "Request rate, ops/s")
	Cmd.Flags().Float64VarP(&config.ReadPercentage, "read-write-percent", "p", 80.0, "Percentage of read requests, compared to total requests")
	Cmd.Flags().Uint32Var(&config.KeysCardinality, "keys-cardinality", 1000, "Batch linger in milliseconds")
	Cmd.Flags().Uint32VarP(&config.ValueSize, "value-size", "s", 128, "Size of the values to write")

	Cmd.Flags().DurationVar(&config.BatchLinger, "batch-linger", oxia.DefaultBatchLinger, "Batch linger time")
	Cmd.Flags().IntVar(&config.MaxRequestsPerBatch, "max-requests-per-batch", oxia.DefaultMaxRequestsPerBatch, "Maximum requests per batch")
	Cmd.Flags().DurationVar(&config.RequestTimeout, "request-timeout", oxia.DefaultRequestTimeout, "Request timeout")
}

func exec(*cobra.Command, []string) {
	process.RunProcess(runPerf)
}

type closer struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newCloser(ctx context.Context) *closer {
	c := &closer{}
	c.ctx, c.cancel = context.WithCancel(ctx)
	return c
}

func (c *closer) Close() error {
	c.cancel()
	return nil
}

func runPerf() (io.Closer, error) {
	closer := newCloser(context.Background())
	go New(config).Run(closer.ctx)
	return closer, nil
}
