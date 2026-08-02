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
	"log/slog"
	"time"

	"github.com/pkg/errors"
	gproto "google.golang.org/protobuf/proto"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	controllerapi "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"
)

type SplitEnsembleSelector func(
	namespace string,
	shard int64,
	editingStatus map[string]commonobject.Borrowed[*proto.NamespaceStatus],
) ([]*proto.DataServerIdentity, error)

type SplitterConfig struct {
	EnsembleSelector SplitEnsembleSelector
	EventListener    controllerapi.ShardSplitEventListener
	SplitTimeout     time.Duration
}

// Splitter validates and persists the initial metadata for a shard split.
// Split is called only from the parent shard controller's event-loop thread,
// serializing it with elections, ensemble changes, and deletion.
type Splitter struct {
	namespace        string
	parentShardID    int64
	metadata         coordmetadata.Metadata
	ensembleSelector SplitEnsembleSelector
	eventListener    controllerapi.ShardSplitEventListener
	logger           *slog.Logger
}

func NewSplitter(
	namespace string,
	parentShardID int64,
	metadata coordmetadata.Metadata,
	config SplitterConfig,
) *Splitter {
	return &Splitter{
		namespace:        namespace,
		parentShardID:    parentShardID,
		metadata:         metadata,
		ensembleSelector: config.EnsembleSelector,
		eventListener:    config.EventListener,
		logger: slog.With(
			slog.String("component", "shard-splitter"),
			slog.String("namespace", namespace),
			slog.Int64("parent-shard", parentShardID),
		),
	}
}

func cloneNamespaceStatuses(
	namespaces map[string]commonobject.Borrowed[*proto.NamespaceStatus],
) map[string]commonobject.Borrowed[*proto.NamespaceStatus] {
	statuses := make(map[string]commonobject.Borrowed[*proto.NamespaceStatus], len(namespaces))
	for namespace, status := range namespaces {
		statuses[namespace] = commonobject.Borrow(
			gproto.Clone(status.UnsafeBorrow()).(*proto.NamespaceStatus),
		)
	}
	return statuses
}

func (s *Splitter) Split(splitPoint *uint32) (leftChildID int64, rightChildID int64, err error) {
	if s.ensembleSelector == nil || s.eventListener == nil {
		return 0, 0, errors.New("shard splitting is not configured")
	}

	status := cloneNamespaceStatuses(s.metadata.ListNamespaceStatus())
	borrowedNS, exists := status[s.namespace]
	if !exists {
		return 0, 0, errors.Errorf("namespace %q not found", s.namespace)
	}
	ns := borrowedNS.UnsafeBorrow()

	parentMeta, exists := ns.Shards[s.parentShardID]
	if !exists {
		return 0, 0, errors.Errorf("shard %d not found in namespace %q", s.parentShardID, s.namespace)
	}
	if parentMeta.GetStatusOrDefault() != proto.ShardStatusSteadyState {
		return 0, 0, errors.Errorf(
			"shard %d is not in steady state (status=%s)",
			s.parentShardID,
			parentMeta.GetStatusOrDefault().String(),
		)
	}
	if parentMeta.Split != nil {
		return 0, 0, errors.Errorf("shard %d already has an active split", s.parentShardID)
	}
	if len(parentMeta.PendingDeleteShardNodes) > 0 {
		return 0, 0, errors.Errorf("shard %d has pending ensemble changes", s.parentShardID)
	}
	if parentMeta.GetInt32HashRange().GetMax()-parentMeta.GetInt32HashRange().GetMin() < 1 {
		return 0, 0, errors.Errorf("shard %d hash range is too small to split", s.parentShardID)
	}

	sp := parentMeta.GetInt32HashRange().GetMin() +
		(parentMeta.GetInt32HashRange().GetMax()-parentMeta.GetInt32HashRange().GetMin())/2
	if splitPoint != nil {
		sp = *splitPoint
		if sp < parentMeta.GetInt32HashRange().GetMin() || sp >= parentMeta.GetInt32HashRange().GetMax() {
			return 0, 0, errors.Errorf(
				"split point %d is outside shard's hash range [%d, %d]",
				sp,
				parentMeta.GetInt32HashRange().GetMin(),
				parentMeta.GetInt32HashRange().GetMax(),
			)
		}
	}

	leftChildID = s.metadata.ReserveShardIDs(2)
	rightChildID = leftChildID + 1

	leftEnsemble, err := s.ensembleSelector(s.namespace, leftChildID, status)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to select ensemble for left child")
	}

	ns.Shards[leftChildID] = &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Ensemble: leftEnsemble,
		Int32HashRange: &proto.HashRange{
			Min: parentMeta.GetInt32HashRange().GetMin(),
			Max: sp,
		},
	}
	rightEnsemble, err := s.ensembleSelector(s.namespace, rightChildID, status)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to select ensemble for right child")
	}

	splitMetadata := &proto.SplitMetadata{
		Phase:         proto.SplitPhaseBootstrap,
		ChildShardIds: []int64{leftChildID, rightChildID},
		SplitPoint:    sp,
	}
	leftChildMetadata := childShardMetadata(
		s.parentShardID,
		parentMeta.GetInt32HashRange().GetMin(),
		sp,
		sp,
		leftEnsemble,
	)
	rightChildMetadata := childShardMetadata(
		s.parentShardID,
		sp+1,
		parentMeta.GetInt32HashRange().GetMax(),
		sp,
		rightEnsemble,
	)

	if err := s.metadata.CreateShardSplit(
		s.namespace,
		s.parentShardID,
		parentMeta,
		splitMetadata,
		map[int64]*proto.ShardMetadata{
			leftChildID:  leftChildMetadata,
			rightChildID: rightChildMetadata,
		},
	); err != nil {
		return 0, 0, err
	}
	s.logger.Info(
		"Split initiated",
		slog.Int64("left-child", leftChildID),
		slog.Int64("right-child", rightChildID),
		slog.Uint64("split-point", uint64(sp)),
	)
	s.eventListener.SplitStarted(s.namespace, s.parentShardID, leftChildID, rightChildID)
	return leftChildID, rightChildID, nil
}

func childShardMetadata(
	parentShardID int64,
	minHash uint32,
	maxHash uint32,
	splitPoint uint32,
	ensemble []*proto.DataServerIdentity,
) *proto.ShardMetadata {
	return &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     0,
		Ensemble: ensemble,
		Int32HashRange: &proto.HashRange{
			Min: minHash,
			Max: maxHash,
		},
		Split: &proto.SplitMetadata{
			Phase:         proto.SplitPhaseBootstrap,
			ParentShardId: parentShardID,
			SplitPoint:    splitPoint,
		},
	}
}
