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

package runtime

import (
	"log/slog"
	"maps"

	"github.com/pkg/errors"
	pb "google.golang.org/protobuf/proto"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	shardcontroller "github.com/oxia-db/oxia/oxiad/coordinator/controller/shard"
)

// InitiateSplit validates and initiates a shard split. It creates child shards
// in the cluster status and starts a SplitController to drive the split.
func (c *runtime) InitiateSplit(namespace string, parentShardId int64, splitPoint *uint32) (leftChild, rightChild int64, err error) {
	c.Lock()
	defer c.Unlock()

	currentStatus, err := c.metadata.ListNamespaceStatus()
	if err != nil {
		return 0, 0, err
	}
	status := cloneNamespaceStatuses(currentStatus)

	// Validate namespace
	borrowedNs, exists := status[namespace]
	if !exists {
		return 0, 0, errors.Errorf("namespace %q not found", namespace)
	}
	ns := borrowedNs.UnsafeBorrow()

	// Validate parent shard
	parentMeta := ns.Shards[parentShardId]
	if err = validateSplitParent(namespace, parentShardId, parentMeta); err != nil {
		return 0, 0, err
	}

	// Compute split point
	var sp uint32
	if splitPoint != nil {
		sp = *splitPoint
		if sp < parentMeta.GetInt32HashRange().GetMin() || sp >= parentMeta.GetInt32HashRange().GetMax() {
			return 0, 0, errors.Errorf("split point %d is outside shard's hash range [%d, %d]",
				sp, parentMeta.GetInt32HashRange().GetMin(), parentMeta.GetInt32HashRange().GetMax())
		}
	} else {
		sp = parentMeta.GetInt32HashRange().GetMin() + (parentMeta.GetInt32HashRange().GetMax()-parentMeta.GetInt32HashRange().GetMin())/2
	}

	// Allocate child shard IDs
	leftChildId, err := c.metadata.AllocateShardIDs(2)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to allocate the child shard ids")
	}
	rightChildId := leftChildId + 1
	// Select ensembles for children.
	// After selecting the left child's ensemble, insert it into the cloned
	// status so the right child's selection sees the updated load distribution
	// and picks a different server.
	// The children inherit the features enabled on the parent, and the split
	// can't complete if the ensemble of a child doesn't support them (see
	// SplitController.addChildObservers): select them among the data servers
	// that support the features of the parent's ensemble.
	parentFeatures := feature.NegotiateDataServers(parentMeta.Ensemble, c.findDataServerFeaturesLocked)
	supportsParentFeatures := func(dataServer *proto.DataServerIdentity) bool {
		supported := c.findDataServerFeaturesLocked([]*proto.DataServerIdentity{dataServer})[dataServer.GetNameOrDefault()]
		return len(feature.Missing(parentFeatures, supported)) == 0
	}
	nsConfig, err := c.namespaceConfigForSplit(namespace)
	if err != nil {
		return 0, 0, err
	}
	leftEnsemble, err := c.selectNewEnsemble(namespace, leftChildId, nsConfig, status, supportsParentFeatures)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to select ensemble for left child")
	}

	// Update cloned status with left child placement before selecting right child
	status[namespace].UnsafeBorrow().Shards[leftChildId] = &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Ensemble: leftEnsemble,
		Int32HashRange: &proto.HashRange{
			Min: parentMeta.GetInt32HashRange().GetMin(),
			Max: sp,
		},
	}
	rightEnsemble, err := c.selectNewEnsemble(namespace, rightChildId, nsConfig, status, supportsParentFeatures)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to select ensemble for right child")
	}

	// Create left child shard
	leftChildMeta := &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     0,
		Ensemble: leftEnsemble,
		Int32HashRange: &proto.HashRange{
			Min: parentMeta.GetInt32HashRange().GetMin(),
			Max: sp,
		},
		Split: &proto.SplitMetadata{
			Phase:         proto.SplitPhaseBootstrap,
			ParentShardId: parentShardId,
			SplitPoint:    sp,
		},
	}

	// Create right child shard
	rightChildMeta := &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     0,
		Ensemble: rightEnsemble,
		Int32HashRange: &proto.HashRange{
			Min: sp + 1,
			Max: parentMeta.GetInt32HashRange().GetMax(),
		},
		Split: &proto.SplitMetadata{
			Phase:         proto.SplitPhaseBootstrap,
			ParentShardId: parentShardId,
			SplitPoint:    sp,
		},
	}

	children := map[int64]*proto.ShardMetadata{leftChildId: leftChildMeta, rightChildId: rightChildMeta}

	// Persist the parent's split metadata and the children in a single status
	// write, applied to the current status: status writers such as the shards'
	// leader elections don't hold the runtime lock, and may have updated them
	// since the status was read above. For the same reason, the parent is
	// validated again.
	parentSplit := &proto.SplitMetadata{
		Phase:         proto.SplitPhaseBootstrap,
		ChildShardIds: []int64{leftChildId, rightChildId},
		SplitPoint:    sp,
	}
	if err := c.metadata.PatchNamespaceStatus(namespace, func(status *proto.NamespaceStatus) (bool, error) {
		shards := status.Shards
		parent := shards[parentShardId]
		if err := validateSplitParent(namespace, parentShardId, parent); err != nil {
			return false, err
		}
		// Create split metadata for parent
		parent.Split = pb.CloneOf(parentSplit)
		maps.Copy(shards, children)
		return true, nil
	}); err != nil {
		return 0, 0, errors.Wrap(err, "failed to persist the split")
	}

	c.logger.Info("Split initiated",
		slog.Int64("parent-shard", parentShardId),
		slog.Int64("left-child", leftChildId),
		slog.Int64("right-child", rightChildId),
		slog.Uint64("split-point", uint64(sp)),
	)

	// Create shard controllers for children
	for childId, childMeta := range children {
		c.shardControllers[childId] = shardcontroller.NewController(namespace, childId, nsConfig,
			childMeta, c.metadata, c.findDataServerFeatures,
			c, c.rpc, shardcontroller.DefaultPeriodicTasksInterval)
	}

	// Start split controller
	sc := shardcontroller.NewSplitController(shardcontroller.SplitControllerConfig{
		Namespace:                 namespace,
		ParentShardId:             parentShardId,
		Metadata:                  c.metadata,
		RpcProvider:               c.rpc,
		EventListener:             c,
		Split:                     parentSplit,
		SupportedFeaturesSupplier: c.findDataServerFeatures,
	})
	c.splitControllers[parentShardId] = sc

	return leftChildId, rightChildId, nil
}

// validateSplitParent checks that the shard with the given metadata, nil if
// it doesn't exist, can be split.
func validateSplitParent(namespace string, parentShardId int64, parentMeta *proto.ShardMetadata) error {
	if parentMeta == nil {
		return errors.Errorf("shard %d not found in namespace %q", parentShardId, namespace)
	}
	if parentMeta.GetStatusOrDefault() != proto.ShardStatusSteadyState {
		return errors.Errorf("shard %d is not in steady state (status=%s)", parentShardId, parentMeta.GetStatus())
	}
	if parentMeta.Split != nil {
		return errors.Errorf("shard %d already has an active split", parentShardId)
	}
	if len(parentMeta.PendingDeleteShardNodes) > 0 {
		return errors.Errorf("shard %d has pending ensemble changes", parentShardId)
	}
	if parentMeta.GetInt32HashRange().GetMax()-parentMeta.GetInt32HashRange().GetMin() < 1 {
		return errors.Errorf("shard %d hash range is too small to split", parentShardId)
	}
	return nil
}

// SplitComplete is called by the SplitController at the end of the Cutover
// phase, after children are re-elected in clean terms and the parent is marked
// Deleting. The coordinator triggers the parent shard's deletion (which retries
// indefinitely until all ensemble members have deleted the shard) and recomputes
// shard assignments so clients discover the children.
//
// NOTE: This is called from within the split controller's own goroutine,
// so we must NOT call sc.Close() on the split controller (that would deadlock
// on wg.Wait). Instead we just remove it from the map and let the goroutine
// finish naturally.
func (c *runtime) SplitComplete(parentShard int64, leftChild int64, rightChild int64) {
	c.Lock()
	defer c.Unlock()

	c.logger.Info("Split complete, triggering parent shard deletion",
		slog.Int64("parent-shard", parentShard),
		slog.Int64("left-child", leftChild),
		slog.Int64("right-child", rightChild),
	)

	// Remove split controller from map without calling Close (would deadlock).
	// The goroutine will return naturally after this callback.
	delete(c.splitControllers, parentShard)

	// Trigger the parent shard controller's deletion. The shard controller
	// retries DeleteShard RPCs indefinitely with backoff, handles unreachable
	// nodes, and removes the parent from cluster status when done.
	if sc, exists := c.shardControllers[parentShard]; exists {
		sc.Delete()
	}

	c.computeNewAssignments()
}

// SplitAborted is called by the SplitController when a split has been
// aborted due to timeout or cancellation. The split controller has already
// cleaned up observer cursors, deleted child shards from status, and
// cleared the parent's split metadata.
func (c *runtime) SplitAborted(parentShard int64, leftChild int64, rightChild int64) {
	c.Lock()
	defer c.Unlock()

	c.logger.Warn("Split aborted",
		slog.Int64("parent-shard", parentShard),
		slog.Int64("left-child", leftChild),
		slog.Int64("right-child", rightChild),
	)

	// Remove split controller from map (goroutine will return after this).
	delete(c.splitControllers, parentShard)

	// Close child shard controllers.
	for _, childId := range []int64{leftChild, rightChild} {
		if sc, exists := c.shardControllers[childId]; exists {
			_ = sc.Close()
			delete(c.shardControllers, childId)
		}
	}

	c.computeNewAssignments()
}

func (c *runtime) namespaceConfigForSplit(namespace string) (*proto.Namespace, error) {
	borrowedNsConfig, exist, err := c.metadata.GetNamespace(namespace)
	if err != nil {
		return nil, err
	}
	if !exist {
		return &proto.Namespace{}, nil
	}
	return borrowedNsConfig.UnsafeBorrow(), nil
}

// restartInProgressSplits checks the cluster status for any shards that have
// active SplitMetadata and creates SplitControllers to resume them.
func (c *runtime) restartInProgressSplits(clusterStatus map[string]commonobject.Borrowed[*proto.NamespaceStatus]) {
	for ns, borrowedShards := range clusterStatus {
		shards := borrowedShards.UnsafeBorrow()
		for shardId, meta := range shards.Shards {
			if meta.Split == nil {
				continue
			}
			// Only create split controller from the parent shard (has ChildShardIds)
			if len(meta.Split.ChildShardIds) == 0 {
				continue
			}

			c.logger.Info("Resuming in-progress split",
				slog.String("namespace", ns),
				slog.Int64("parent-shard", shardId),
				slog.String("phase", meta.Split.GetPhaseOrDefault().String()),
			)

			sc := shardcontroller.NewSplitController(shardcontroller.SplitControllerConfig{
				Namespace:                 ns,
				ParentShardId:             shardId,
				Metadata:                  c.metadata,
				RpcProvider:               c.rpc,
				EventListener:             c,
				Split:                     meta.Split,
				SupportedFeaturesSupplier: c.findDataServerFeatures,
			})
			c.splitControllers[shardId] = sc
		}
	}
}
