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

package dataserver

import (
	"context"
	"time"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller"
)

type checksumScheduler struct {
	interval       time.Duration
	shardsDirector controller.ShardsDirector
	ctx            context.Context
}

func newChecksumScheduler(ctx context.Context, interval time.Duration, sd controller.ShardsDirector) *checksumScheduler {
	return &checksumScheduler{
		interval:       interval,
		shardsDirector: sd,
		ctx:            ctx,
	}
}

func (cs *checksumScheduler) run() {
	process.DoWithLabels(
		cs.ctx,
		map[string]string{
			"oxia": "checksum-scheduler",
		},
		func() {
			ticker := time.NewTicker(cs.interval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					cs.recordChecksums()
				case <-cs.ctx.Done():
					return
				}
			}
		},
	)
}

func (cs *checksumScheduler) recordChecksums() {
	for _, leader := range cs.shardsDirector.GetAllLeaders() {
		if leader.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM) {
			leader.ProposeRecordChecksum(cs.ctx)
		}
	}
}
