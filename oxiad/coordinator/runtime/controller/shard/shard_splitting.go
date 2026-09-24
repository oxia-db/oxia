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

package shard

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/pkg/errors"

	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	controllerapi "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"

	"github.com/oxia-db/oxia/common/proto"
)

var ErrSplitInProgress = errors.New("the shard already has a split in progress")

// Splitting is one round of splitting a shard in two, the way Election is one
// round of electing a leader for it: it is owned by the shard controller,
// which holds at most one at a time and replaces it wholesale rather than
// resetting it.
//
// A round is either started fresh, where it records the split in the cluster
// status before driving it, or resumed from a split that was already recorded
// and left unfinished by a previous coordinator.
type Splitting struct {
	logger    *slog.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc
	// borrowed resource
	metadataStore                       coordmetadata.Metadata
	provider                            rpc.Provider
	eventListener                       controllerapi.ShardSplitEventListener
	dataServerSupportedFeaturesSupplier DataServerSupportedFeaturesSupplier
	// owned status
	namespace   string
	parentShard int64
	left        int64
	right       int64
	splitPoint  uint32
	// recorded reports whether the split is already in the cluster status: a
	// resumed round does not record it again.
	recorded bool
	// driver runs the split phases. They still live in SplitController, on its
	// own goroutine, until they move onto this round.
	driver *SplitController
	// started
	started atomic.Bool
}

//nolint:revive
func NewSplitting(ctx context.Context, logger *slog.Logger,
	eventListener controllerapi.ShardSplitEventListener,
	metadataStore coordmetadata.Metadata,
	dataServerSupportedFeaturesSupplier DataServerSupportedFeaturesSupplier,
	provider rpc.Provider, namespace string, parentShard, left, right int64,
	splitPoint uint32) *Splitting {
	return newSplitting(ctx, logger, eventListener, metadataStore, dataServerSupportedFeaturesSupplier,
		provider, namespace, parentShard, left, right, splitPoint, false)
}

// ResumeSplitting picks up a split that is already recorded in the cluster
// status, continuing from the phase it reached.
//
//nolint:revive
func ResumeSplitting(ctx context.Context, logger *slog.Logger,
	eventListener controllerapi.ShardSplitEventListener,
	metadataStore coordmetadata.Metadata,
	dataServerSupportedFeaturesSupplier DataServerSupportedFeaturesSupplier,
	provider rpc.Provider, namespace string, parentShard int64,
	split *proto.SplitMetadata) *Splitting {
	return newSplitting(ctx, logger, eventListener, metadataStore, dataServerSupportedFeaturesSupplier,
		provider, namespace, parentShard, split.GetChildShardIds()[0], split.GetChildShardIds()[1],
		split.GetSplitPoint(), true)
}

//nolint:revive
func newSplitting(ctx context.Context, logger *slog.Logger,
	eventListener controllerapi.ShardSplitEventListener,
	metadataStore coordmetadata.Metadata,
	dataServerSupportedFeaturesSupplier DataServerSupportedFeaturesSupplier,
	provider rpc.Provider, namespace string, parentShard, left, right int64,
	splitPoint uint32, recorded bool) *Splitting {
	current, cancelFunc := context.WithCancel(ctx)
	return &Splitting{
		logger: logger.With(
			slog.Int64("left-child", left),
			slog.Int64("right-child", right),
		),
		ctx:                                 current,
		ctxCancel:                           cancelFunc,
		metadataStore:                       metadataStore,
		provider:                            provider,
		eventListener:                       eventListener,
		dataServerSupportedFeaturesSupplier: dataServerSupportedFeaturesSupplier,
		namespace:                           namespace,
		parentShard:                         parentShard,
		left:                                left,
		right:                               right,
		splitPoint:                          splitPoint,
		recorded:                            recorded,
	}
}

// Start records the split, unless it is being resumed, and then drives it.
// Recording is what makes the split visible to a restarted coordinator, so it
// happens before anything else: nothing has been done to the shards until it
// succeeds, and a failure leaves the parent untouched.
func (s *Splitting) Start(leftEnsemble, rightEnsemble []*proto.DataServerIdentity) error {
	if swapped := s.started.CompareAndSwap(false, true); !swapped {
		panic("bug! the splitting has been started")
	}

	if !s.recorded {
		if err := s.metadataStore.InitShardSplit(s.namespace, s.parentShard, s.left, s.right,
			s.splitPoint, leftEnsemble, rightEnsemble); err != nil {
			return errors.Wrap(err, "failed to record the split")
		}
		s.logger.Info("Split recorded", slog.Uint64("split-point", uint64(s.splitPoint)))
	}

	s.driver = NewSplitController(SplitControllerConfig{
		Namespace:                 s.namespace,
		ParentShardId:             s.parentShard,
		Metadata:                  s.metadataStore,
		RpcProvider:               s.provider,
		EventListener:             s.eventListener,
		SupportedFeaturesSupplier: s.dataServerSupportedFeaturesSupplier,
	})
	return nil
}

// finish releases the round after it reported its own outcome. Unlike Stop it
// does not close the driver: the driver's goroutine is the one reporting, and
// waiting for it here would wait on the caller of this method.
func (s *Splitting) finish() {
	s.ctxCancel()
}

func (s *Splitting) Stop() {
	s.ctxCancel()
	if s.driver != nil {
		s.driver.Close()
	}
	s.logger.Info("stopped the splitting")
}
