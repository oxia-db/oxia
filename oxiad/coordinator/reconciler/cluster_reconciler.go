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

package reconciler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime"
)

var _ Reconciler = &clusterReconciler{}

type clusterReconciler struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup
	logger    *slog.Logger

	runtime     runtime.Runtime
	reconcilers []Reconciler
}

func New(ctx context.Context, coordinatorRuntime runtime.Runtime) Reconciler {
	reconcilerCtx, cancel := context.WithCancel(ctx)
	r := &clusterReconciler{
		ctx:       reconcilerCtx,
		ctxCancel: cancel,
		wg:        sync.WaitGroup{},
		logger:    slog.With(slog.String("component", "reconciler")),
		runtime:   coordinatorRuntime,
		reconcilers: []Reconciler{
			&dataServerReconciler{runtime: coordinatorRuntime},
			&namespaceReconciler{runtime: coordinatorRuntime},
		},
	}

	subscription := r.runtime.Metadata().SubscribeConfig()
	r.reconcile0(subscription.Get().Value, subscription)

	r.wg.Go(func() {
		defer subscription.Close()
		process.DoWithLabels(reconcilerCtx, map[string]string{
			"component": "coordinator-reconciler",
		}, func() { r.bgWatchClusterConfiguration(subscription) })
	})

	return r
}

func (r *clusterReconciler) Close() error {
	r.ctxCancel()
	r.wg.Wait()

	var err error
	for _, reconciler := range r.reconcilers {
		err = multierr.Append(err, reconciler.Close())
	}
	return err
}

func (r *clusterReconciler) Reconcile(_ context.Context, snapshot *proto.ClusterConfiguration) error {
	for _, reconciler := range r.reconcilers {
		if err := reconciler.Reconcile(r.ctx, snapshot); err != nil {
			return err
		}
	}
	r.runtime.RecomputeAssignments()
	return nil
}

func (r *clusterReconciler) bgWatchClusterConfiguration(subscription *cache.Subscription[provider.Versioned[*proto.ClusterConfiguration]]) {
	for {
		select {
		case <-r.ctx.Done():
			return
		case _, ok := <-subscription.Changed():
			if !ok {
				return
			}
			r.reconcile0(subscription.Get().Value, subscription)
		}
	}
}

func (r *clusterReconciler) reconcile0(snapshot *proto.ClusterConfiguration, subscription *cache.Subscription[provider.Versioned[*proto.ClusterConfiguration]]) {
	_ = backoff.RetryNotify(func() error {
		// update the snapshot when we are retrying
		select {
		case _, ok := <-subscription.Changed():
			if ok {
				snapshot = subscription.Get().Value
			}
		default:
		}
		return r.Reconcile(r.ctx, snapshot)
	}, oxiatime.NewBackOffWithInitialInterval(r.ctx, time.Second), func(err error, retryAfter time.Duration) {
		r.logger.Warn("failed to reconcile config update",
			slog.Any("error", err),
			slog.Duration("retry-after", retryAfter))
	})
}
