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

package reconciler

import (
	"context"
	"log/slog"

	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
)

type ConfigReconciler struct {
	log   *slog.Logger
	watch *commonwatch.Watch[*proto.ClusterConfiguration]
	apply func(*proto.ClusterConfiguration)
}

func NewConfigReconciler(
	log *slog.Logger,
	watch *commonwatch.Watch[*proto.ClusterConfiguration],
	apply func(*proto.ClusterConfiguration),
) *ConfigReconciler {
	return &ConfigReconciler{
		log:   log,
		watch: watch,
		apply: apply,
	}
}

func (r *ConfigReconciler) Run(ctx context.Context) {
	receiver, err := r.watch.Subscribe()
	if err != nil {
		r.log.Warn("failed to subscribe to cluster config watch", slog.Any("error", err))
		return
	}
	defer func() {
		_ = receiver.Close()
	}()

	r.applyCurrent(receiver)
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-receiver.Changed():
			if !ok {
				return
			}
		}

		r.applyCurrent(receiver)
	}
}

func (r *ConfigReconciler) applyCurrent(receiver *commonwatch.Receiver[*proto.ClusterConfiguration]) {
	config, ok := receiver.Load()
	if !ok || config == nil {
		return
	}
	r.apply(config)
}
