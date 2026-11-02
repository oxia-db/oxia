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
	"github.com/pkg/errors"
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

// New returns the cluster reconciler of the runtime. It reconciles the current
// configuration before returning, and fails if that pass fails, then reconciles
// each change of the configuration in the background, retrying a failed pass
// with a backoff until it is closed.
func New(ctx context.Context, coordinatorRuntime runtime.Runtime) (Reconciler, error) {
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

	config, err := r.runtime.Metadata().GetConfig()
	if err != nil {
		return nil, multierr.Append(errors.Wrap(err, "failed to load the cluster configuration"), r.Close())
	}
	if err := r.Reconcile(reconcilerCtx, config.UnsafeBorrow()); err != nil {
		return nil, multierr.Append(errors.Wrap(err, "failed to reconcile the cluster configuration"), r.Close())
	}

	// The watch reconciles the current configuration before waiting for its
	// changes, so a change made since the first reconciliation isn't missed
	subscription := r.runtime.Metadata().SubscribeConfig()
	r.wg.Go(func() {
		defer subscription.Close()
		process.DoWithLabels(reconcilerCtx, map[string]string{
			"component": "coordinator-reconciler",
		}, func() {
			r.reconcileWithRetries(subscription,
				oxiatime.NewBackOffWithInitialInterval(reconcilerCtx, time.Second))
		})
	})

	return r, nil
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
	if unknown := snapshot.ListUnknownAnnotations(); len(unknown) > 0 {
		r.logger.Warn("Ignoring the unknown annotations of the configuration", slog.Any("annotations", unknown))
	}
	var errs error
	for _, reconciler := range r.reconcilers {
		if err := reconciler.Reconcile(r.ctx, snapshot); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	r.runtime.RecomputeAssignments()
	return errs
}

func (r *clusterReconciler) reconcileWithRetries(
	subscription *cache.Subscription[provider.Versioned[*proto.ClusterConfiguration]], bo backoff.BackOff) {
	_ = backoff.RetryNotify(func() error {
		for {
			config, err := r.runtime.Metadata().GetConfig()
			if err != nil {
				return err
			}
			if err := r.Reconcile(r.ctx, config.UnsafeBorrow()); err != nil {
				return err
			}
			bo.Reset()
			select {
			case <-r.ctx.Done():
				return nil
			case _, ok := <-subscription.Changed():
				if !ok {
					return nil
				}
			}
		}
	}, bo, func(err error, retryAfter time.Duration) {
		r.logger.Warn("failed to reconcile config update",
			slog.Any("error", err),
			slog.Duration("retry-after", retryAfter))
	})
	r.logger.Info("Cluster configuration watcher closed")
}
