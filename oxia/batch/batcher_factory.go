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

package batch

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/oxia-db/oxia/common/process"
)

var batcherChannelBufferSize = runtime.GOMAXPROCS(-1)

type BatcherFactory struct {
	Linger              time.Duration
	MaxRequestsPerBatch int
}

func (b *BatcherFactory) NewBatcher(ctx context.Context, shard int64, batcherType string, batchFactory func() Batch) Batcher {
	batcher := &batcherImpl{
		batchFactory:        batchFactory,
		callC:               make(chan any, batcherChannelBufferSize),
		closeC:              make(chan bool),
		linger:              b.Linger,
		maxRequestsPerBatch: b.MaxRequestsPerBatch,
	}

	go process.DoWithLabels(ctx, map[string]string{
		"oxia":  fmt.Sprintf("batcher-%s", batcherType),
		"shard": fmt.Sprintf("%d", shard),
	}, batcher.Run)

	return batcher
}
