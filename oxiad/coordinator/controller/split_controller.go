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

package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/resource"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
)

// SplitEventListener is notified when split phases complete or abort.
type SplitEventListener interface {
	// SplitComplete is called at the end of the cutover phase, after children
	// are re-elected in clean terms and the parent is marked Deleting. The
	// coordinator should close the parent ShardController and recompute
	// shard assignments so clients discover the children.
	SplitComplete(parentShard int64, leftChild int64, rightChild int64)

	// SplitAborted is called when the split has been aborted (e.g. due to
	// timeout). The coordinator should close child shard controllers and
	// recompute shard assignments.
	SplitAborted(parentShard int64, leftChild int64, rightChild int64)
}

// SplitController drives the shard split state machine through 4 phases
// (Bootstrap → CatchUp → Cutover → Cleanup). It runs alongside the parent's
// ShardController.
type SplitController struct {
	namespace     string
	parentShardId int64
	leftChildId   int64
	rightChildId  int64
	splitPoint    uint32

	statusResource resource.StatusResource
	rpcProvider    rpc.Provider
	eventListener  SplitEventListener

	// ensembleSelector selects server ensembles for new shards.
	ensembleSelector func(namespace string) ([]model.Server, error)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	log    *slog.Logger
}

const DefaultSplitTimeout = 5 * time.Minute

// SplitControllerConfig holds the configuration needed to create a SplitController.
type SplitControllerConfig struct {
	Namespace        string
	ParentShardId    int64
	StatusResource   resource.StatusResource
	RpcProvider      rpc.Provider
	EventListener    SplitEventListener
	EnsembleSelector func(namespace string) ([]model.Server, error)

	// SplitTimeout is the maximum duration for the entire split operation.
	// If the split does not complete within this time, it is aborted.
	// Zero means use DefaultSplitTimeout.
	SplitTimeout time.Duration
}

// NewSplitController creates a new SplitController and starts it running
// in the background. It will pick up from whatever phase is persisted in
// the cluster status.
func NewSplitController(cfg SplitControllerConfig) *SplitController {
	sc := &SplitController{
		namespace:        cfg.Namespace,
		parentShardId:    cfg.ParentShardId,
		statusResource:   cfg.StatusResource,
		rpcProvider:      cfg.RpcProvider,
		eventListener:    cfg.EventListener,
		ensembleSelector: cfg.EnsembleSelector,
		log: slog.With(
			slog.String("component", "split-controller"),
			slog.String("namespace", cfg.Namespace),
			slog.Int64("parent-shard", cfg.ParentShardId),
		),
	}

	splitTimeout := cfg.SplitTimeout
	if splitTimeout == 0 {
		splitTimeout = DefaultSplitTimeout
	}
	sc.ctx, sc.cancel = context.WithTimeout(context.Background(), splitTimeout)

	// Load the current split metadata from cluster status
	status := sc.statusResource.Load()
	ns, exists := status.Namespaces[sc.namespace]
	if !exists {
		sc.log.Error("Namespace not found in cluster status")
		return sc
	}
	parentMeta, exists := ns.Shards[sc.parentShardId]
	if !exists || parentMeta.Split == nil {
		sc.log.Error("Parent shard or split metadata not found")
		return sc
	}

	sc.leftChildId = parentMeta.Split.ChildShardIDs[0]
	sc.rightChildId = parentMeta.Split.ChildShardIDs[1]
	sc.splitPoint = parentMeta.Split.SplitPoint

	sc.wg.Go(func() {
		process.DoWithLabels(
			sc.ctx,
			map[string]string{
				"oxia":      "split-controller",
				"namespace": sc.namespace,
				"parent":    fmt.Sprintf("%d", sc.parentShardId),
			},
			sc.run,
		)
	})

	return sc
}

func (sc *SplitController) Close() {
	sc.cancel()
	sc.wg.Wait()
}

func (sc *SplitController) run() {
	_ = backoff.RetryNotify(func() error {
		return sc.driveStateMachine()
	}, oxiatime.NewBackOff(sc.ctx), func(err error, duration time.Duration) {
		sc.log.Warn(
			"Split state machine step failed, retrying",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})

	// If we exited due to timeout/cancellation and the split isn't done,
	// abort and clean up.
	if sc.ctx.Err() != nil {
		phase := sc.currentPhase()
		if phase != nil && *phase < model.SplitPhaseCutover {
			sc.abort()
		}
	}
}

func (sc *SplitController) driveStateMachine() error {
	for {
		if err := sc.ctx.Err(); err != nil {
			return backoff.Permanent(err)
		}

		phase := sc.currentPhase()
		if phase == nil {
			// Split is done or metadata was cleaned up
			return nil
		}

		sc.log.Info("Running split phase", slog.String("phase", phase.String()))

		var err error
		switch *phase {
		case model.SplitPhaseBootstrap:
			err = sc.runBootstrap()
		case model.SplitPhaseCatchUp:
			err = sc.runCatchUp()
		case model.SplitPhaseCutover:
			err = sc.runCutover()
		default:
			sc.log.Error("Unknown split phase", slog.Any("phase", *phase))
			return nil
		}

		if err != nil {
			return err
		}
	}
}

func (sc *SplitController) currentPhase() *model.SplitPhase {
	status := sc.statusResource.Load()
	ns, exists := status.Namespaces[sc.namespace]
	if !exists {
		return nil
	}
	parentMeta, exists := ns.Shards[sc.parentShardId]
	if !exists || parentMeta.Split == nil {
		return nil
	}
	return &parentMeta.Split.Phase
}

// updatePhase atomically updates the split phase on both parent and children.
func (sc *SplitController) updatePhase(newPhase model.SplitPhase) error {
	status := sc.statusResource.Load()
	cloned := status.Clone()

	ns := cloned.Namespaces[sc.namespace]

	// Update parent
	if parentMeta, exists := ns.Shards[sc.parentShardId]; exists && parentMeta.Split != nil {
		parentMeta.Split.Phase = newPhase
		ns.Shards[sc.parentShardId] = parentMeta
	}

	// Update children
	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		if childMeta, exists := ns.Shards[childId]; exists && childMeta.Split != nil {
			childMeta.Split.Phase = newPhase
			ns.Shards[childId] = childMeta
		}
	}

	return sc.statusResource.Update(cloned)
}

// runBootstrap validates preconditions, fences child ensemble members, elects
// child leaders (so they start replicating to their followers immediately),
// and adds children as observer followers on the parent leader.
func (sc *SplitController) runBootstrap() error {
	sc.log.Info("Phase Bootstrap: fencing children and adding as observers")

	parentMeta := sc.loadParentMeta()
	if parentMeta == nil || parentMeta.Leader == nil {
		return errors.New("parent shard has no leader")
	}
	if parentMeta.Status != model.ShardStatusSteadyState {
		return errors.New("parent shard is not in steady state")
	}

	// Step 1: Fence and elect each child leader (if not already done).
	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		if err := sc.fenceAndElectChild(childId); err != nil {
			return err
		}
	}

	// Step 2: Add each child leader as an observer on the parent leader.
	// Re-read parent metadata in case the parent leader changed while
	// fencing children.
	parentMeta = sc.loadParentMeta()
	if parentMeta == nil || parentMeta.Leader == nil {
		return errors.New("parent shard has no leader")
	}
	parentLeader := *parentMeta.Leader
	parentTerm := parentMeta.Term

	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		if err := sc.addChildObserver(childId, parentLeader, parentTerm); err != nil {
			return err
		}
	}

	// Record the parent term and child leaders used during bootstrap so
	// CatchUp can detect if a parent or child leader election invalidated
	// the observer cursors.
	childLeaders := make(map[int64]string)
	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		childMeta := sc.loadShardMeta(childId)
		if childMeta != nil && childMeta.Leader != nil {
			childLeaders[childId] = childMeta.Leader.Internal
		}
	}
	if err := sc.updateParentMeta(func(meta *model.ShardMetadata) {
		meta.Split.ParentTermAtBootstrap = parentTerm
		meta.Split.ChildLeadersAtBootstrap = childLeaders
	}); err != nil {
		return err
	}

	return sc.updatePhase(model.SplitPhaseCatchUp)
}

// fenceAndElectChild fences a child shard's ensemble and elects a leader.
// Skipped if the child already has a leader (from a previous Bootstrap run).
func (sc *SplitController) fenceAndElectChild(childId int64) error {
	childMeta := sc.loadShardMeta(childId)
	if childMeta == nil {
		return errors.Errorf("child shard %d not found", childId)
	}

	if childMeta.Leader != nil {
		sc.log.Info("Child already has leader, skipping fence/elect",
			slog.Int64("child-shard", childId),
			slog.Any("leader", *childMeta.Leader),
		)
		return nil
	}

	childTerm := childMeta.Term + 1
	headEntries, err := sc.fenceEnsemble(childId, childTerm, childMeta.Ensemble)
	if err != nil {
		return errors.Wrapf(err, "failed to fence child shard %d", childId)
	}

	childLeader := sc.pickLeader(headEntries)

	if err := sc.updateChildMeta(childId, func(meta *model.ShardMetadata) {
		meta.Term = childTerm
		meta.Leader = &childLeader
		meta.Status = model.ShardStatusSteadyState
	}); err != nil {
		return err
	}

	// Elect the child leader so it replicates to its followers immediately.
	// Without this, only the single child leader node has the data.
	followerMap := make(map[string]*proto.EntryId)
	for server, entry := range headEntries {
		if server != childLeader {
			followerMap[server.Internal] = entry
		}
	}

	_, err = sc.rpcProvider.BecomeLeader(sc.ctx, childLeader, &proto.BecomeLeaderRequest{
		Namespace:         sc.namespace,
		Shard:             childId,
		Term:              childTerm,
		ReplicationFactor: uint32(len(childMeta.Ensemble)),
		FollowerMaps:      followerMap,
	})
	if err != nil {
		return errors.Wrapf(err, "BecomeLeader failed for child %d", childId)
	}

	sc.log.Info("Child leader elected",
		slog.Int64("child-shard", childId),
		slog.Any("child-leader", childLeader),
		slog.Int64("term", childTerm),
	)
	return nil
}

// addChildObserver adds a child's leader as an observer follower on the parent
// leader so the parent streams snapshots and WAL entries to it.
func (sc *SplitController) addChildObserver(childId int64, parentLeader model.Server, parentTerm int64) error {
	childMeta := sc.loadShardMeta(childId)
	if childMeta == nil || childMeta.Leader == nil {
		return errors.Errorf("child shard %d has no leader", childId)
	}
	childLeader := *childMeta.Leader

	_, err := sc.rpcProvider.AddFollower(sc.ctx, parentLeader, &proto.AddFollowerRequest{
		Namespace:    sc.namespace,
		Shard:        sc.parentShardId,
		Term:         parentTerm,
		FollowerName: childLeader.Internal,
		FollowerHeadEntryId: &proto.EntryId{
			Term:   -1,
			Offset: -1,
		},
		Observer:    true,
		TargetShard: &childId,
		SplitHashRange: &proto.Int32HashRange{
			MinHashInclusive: childMeta.Int32HashRange.Min,
			MaxHashInclusive: childMeta.Int32HashRange.Max,
		},
	})
	if err != nil {
		return errors.Wrapf(err, "failed to add child %d as observer on parent", childId)
	}

	sc.log.Info("Added child as observer on parent",
		slog.Int64("child-shard", childId),
		slog.Any("child-leader", childLeader),
	)
	return nil
}

// CatchUpRoundTimeout is the maximum time to wait for children to reach a
// snapshot of the parent's commitOffset. If the round times out, re-read
// the parent's commitOffset and try again.
const CatchUpRoundTimeout = 10 * time.Second

// runCatchUp monitors children's commitOffset until they reach the parent's
// current position. Uses a round-based algorithm: snapshot the parent's
// commitOffset, wait up to 10s for both children to reach it. If the round
// expires (parent under heavy write load), re-read and try again.
//
// We check commitOffset (not headOffset) because the children were elected
// leader during Bootstrap and are actively replicating to their followers.
// commitOffset advancing means a quorum of child followers have the data.
func (sc *SplitController) runCatchUp() error {
	sc.log.Info("Phase CatchUp: monitoring observer progress")

	for {
		if err := sc.ctx.Err(); err != nil {
			return backoff.Permanent(err)
		}

		if fallback, err := sc.checkObserverCursorsStale(); err != nil {
			return err
		} else if fallback {
			return nil
		}

		caughtUp, err := sc.runCatchUpRound()
		if err != nil {
			return err
		}
		if caughtUp {
			sc.log.Info("All children caught up")
			return sc.updatePhase(model.SplitPhaseCutover)
		}
	}
}

// checkObserverCursorsStale detects if a parent or child leader election has
// invalidated the observer cursors set up during Bootstrap. Returns
// (true, nil) if the phase was reset to Bootstrap and the caller should return.
func (sc *SplitController) checkObserverCursorsStale() (bool, error) {
	parentMeta := sc.loadParentMeta()
	if parentMeta == nil || parentMeta.Split == nil {
		return false, errors.New("parent or split metadata missing")
	}

	// Parent leader election: observer cursors are closed when the old leader
	// is fenced, so they need to be re-added on the new leader.
	if parentMeta.Split.ParentTermAtBootstrap > 0 && parentMeta.Term != parentMeta.Split.ParentTermAtBootstrap {
		sc.log.Warn("Parent term changed since bootstrap, resetting to Bootstrap",
			slog.Int64("bootstrap-term", parentMeta.Split.ParentTermAtBootstrap),
			slog.Int64("current-term", parentMeta.Term),
		)
		if err := sc.updatePhase(model.SplitPhaseBootstrap); err != nil {
			return false, err
		}
		return true, nil
	}

	// Child leader election: the observer cursor targets the old (dead) leader.
	// Remove the stale cursor and fall back to Bootstrap to re-add.
	if sc.removeStaleChildObservers(parentMeta) {
		if err := sc.updatePhase(model.SplitPhaseBootstrap); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

// removeStaleChildObservers checks if any child leader changed since Bootstrap.
// If so, removes the stale observer cursor from the parent and returns true.
func (sc *SplitController) removeStaleChildObservers(parentMeta *model.ShardMetadata) bool {
	if parentMeta.Split.ChildLeadersAtBootstrap == nil || parentMeta.Leader == nil {
		return false
	}
	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		childMeta := sc.loadShardMeta(childId)
		if childMeta == nil || childMeta.Leader == nil {
			continue
		}
		bootstrapLeader, ok := parentMeta.Split.ChildLeadersAtBootstrap[childId]
		if !ok || childMeta.Leader.Internal == bootstrapLeader {
			continue
		}

		sc.log.Warn("Child leader changed since bootstrap, removing stale observer and resetting to Bootstrap",
			slog.Int64("child-shard", childId),
			slog.String("old-leader", bootstrapLeader),
			slog.String("new-leader", childMeta.Leader.Internal),
		)
		_, _ = sc.rpcProvider.RemoveObserver(sc.ctx, *parentMeta.Leader, &proto.RemoveObserverRequest{
			Namespace:    sc.namespace,
			Shard:        sc.parentShardId,
			Term:         parentMeta.Term,
			FollowerName: bootstrapLeader,
			TargetShard:  childId,
		})
		return true
	}
	return false
}

// runCatchUpRound snapshots the parent's commitOffset and waits up to
// CatchUpRoundTimeout for both children to reach it. Returns true if all
// children caught up, false if the round timed out (caller should retry).
func (sc *SplitController) runCatchUpRound() (bool, error) {
	parentMeta := sc.loadParentMeta()
	if parentMeta == nil || parentMeta.Leader == nil {
		return false, errors.New("parent has no leader")
	}

	parentStatus, err := sc.rpcProvider.GetStatus(sc.ctx, *parentMeta.Leader, &proto.GetStatusRequest{
		Shard: sc.parentShardId,
	})
	if err != nil {
		return false, err
	}
	target := parentStatus.CommitOffset

	sc.log.Info("CatchUp round: waiting for children to reach target",
		slog.Int64("target-commit-offset", target),
	)

	roundCtx, roundCancel := context.WithTimeout(sc.ctx, CatchUpRoundTimeout)
	defer roundCancel()

	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		if err := sc.waitForChildCommitOffset(roundCtx, childId, target); err != nil {
			if roundCtx.Err() != nil {
				sc.log.Info("CatchUp round timed out, retrying",
					slog.Int64("child-shard", childId),
					slog.Int64("target", target),
				)
				return false, nil
			}
			return false, err
		}
	}
	return true, nil
}

// runCutover fences the parent, waits for children to commit all remaining
// data, re-elects child leaders in a clean term, updates shard assignments,
// and marks the parent for deletion.
func (sc *SplitController) runCutover() error {
	sc.log.Info("Phase Cutover: fencing parent and completing split")

	parentMeta := sc.loadParentMeta()
	if parentMeta == nil {
		return errors.New("parent shard not found")
	}

	// Step 1: Fence the parent (increment term, send NewTerm).
	// This stops the parent from accepting writes and kills observer cursors.
	newParentTerm := parentMeta.Term + 1
	parentHeadEntries, err := sc.fenceEnsemble(sc.parentShardId, newParentTerm, parentMeta.Ensemble)
	if err != nil {
		return errors.Wrap(err, "failed to fence parent during cutover")
	}

	// Find the parent's final head offset
	var parentFinalOffset int64 = -1
	for _, entry := range parentHeadEntries {
		if entry.Offset > parentFinalOffset {
			parentFinalOffset = entry.Offset
		}
	}

	sc.log.Info("Parent fenced",
		slog.Int64("new-term", newParentTerm),
		slog.Int64("final-offset", parentFinalOffset),
	)

	// Update parent term in metadata
	if err := sc.updateParentMeta(func(meta *model.ShardMetadata) {
		meta.Term = newParentTerm
		meta.Leader = nil
		meta.Status = model.ShardStatusElection
	}); err != nil {
		return err
	}

	// Step 2: Wait for children to commit parentFinalOffset.
	// Children were already elected leader in Bootstrap, so commitOffset
	// advances as their followers acknowledge.
	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		if err := sc.waitForChildCommitOffset(sc.ctx, childId, parentFinalOffset); err != nil {
			return errors.Wrapf(err, "child %d failed to commit parent final offset", childId)
		}
	}

	// Step 3: Re-elect child leaders in a clean term (independent of parent).
	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		if err := sc.reelectChild(childId); err != nil {
			return errors.Wrapf(err, "failed to re-elect child %d leader", childId)
		}
	}

	// Step 4: Clear split metadata from children and mark parent for deletion.
	// Children are now independent shards.
	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		if err := sc.updateChildMeta(childId, func(meta *model.ShardMetadata) {
			meta.Split = nil
		}); err != nil {
			return err
		}
	}

	if err := sc.updateParentMeta(func(meta *model.ShardMetadata) {
		meta.Status = model.ShardStatusDeleting
	}); err != nil {
		return err
	}

	// Step 5: Notify the coordinator. This triggers the parent shard
	// controller's DeleteShard (which retries indefinitely with backoff)
	// and recomputes shard assignments so clients discover the children.
	sc.eventListener.SplitComplete(sc.parentShardId, sc.leftChildId, sc.rightChildId)

	// Clear split metadata from parent — the split controller's job is done.
	// The parent shard controller handles the actual deletion.
	return sc.updateParentMeta(func(meta *model.ShardMetadata) {
		meta.Split = nil
	})
}

// abort cleans up a failed/timed-out split that hasn't reached Cutover.
// It removes observer cursors from the parent, deletes child shards from
// status, clears the parent's split metadata, and notifies the coordinator.
func (sc *SplitController) abort() {
	sc.log.Warn("Aborting split due to timeout or cancellation")

	// Use a fresh context since the split context is cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	parentMeta := sc.loadParentMeta()

	// Remove observer cursors from parent leader (best-effort).
	// Only needed if we reached Bootstrap (observers were added).
	if parentMeta != nil && parentMeta.Split != nil && parentMeta.Leader != nil {
		phase := parentMeta.Split.Phase
		if phase >= model.SplitPhaseBootstrap {
			for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
				childMeta := sc.loadShardMeta(childId)
				if childMeta != nil && childMeta.Leader != nil {
					_, err := sc.rpcProvider.RemoveObserver(ctx, *parentMeta.Leader, &proto.RemoveObserverRequest{
						Namespace:    sc.namespace,
						Shard:        sc.parentShardId,
						Term:         parentMeta.Term,
						FollowerName: childMeta.Leader.Internal,
						TargetShard:  childId,
					})
					if err != nil {
						sc.log.Warn("Failed to remove observer during abort",
							slog.Int64("child-shard", childId),
							slog.Any("error", err),
						)
					}
				}
			}
		}
	}

	// Delete child shards from status.
	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		if err := sc.statusResource.DeleteShardMetadata(sc.namespace, childId); err != nil {
			sc.log.Warn("Failed to delete child shard metadata during abort",
				slog.Int64("child-shard", childId),
				slog.Any("error", err))
		}
	}

	// Clear parent split metadata.
	if err := sc.updateParentMeta(func(meta *model.ShardMetadata) {
		meta.Split = nil
	}); err != nil {
		sc.log.Warn("Failed to clear parent split metadata during abort", slog.Any("error", err))
	}

	sc.log.Info("Split aborted, parent restored")

	// Notify coordinator to clean up child controllers and recompute assignments.
	sc.eventListener.SplitAborted(sc.parentShardId, sc.leftChildId, sc.rightChildId)
}

// --- Helper methods ---

func (sc *SplitController) loadParentMeta() *model.ShardMetadata {
	return sc.loadShardMeta(sc.parentShardId)
}

func (sc *SplitController) loadShardMeta(shardId int64) *model.ShardMetadata {
	status := sc.statusResource.Load()
	ns, exists := status.Namespaces[sc.namespace]
	if !exists {
		return nil
	}
	meta, exists := ns.Shards[shardId]
	if !exists {
		return nil
	}
	cloned := meta.Clone()
	return &cloned
}

func (sc *SplitController) updateParentMeta(fn func(meta *model.ShardMetadata)) error {
	return sc.updateShardMeta(sc.parentShardId, fn)
}

func (sc *SplitController) updateChildMeta(childId int64, fn func(meta *model.ShardMetadata)) error {
	return sc.updateShardMeta(childId, fn)
}

func (sc *SplitController) updateShardMeta(shardId int64, fn func(meta *model.ShardMetadata)) error {
	status := sc.statusResource.Load()
	cloned := status.Clone()
	ns := cloned.Namespaces[sc.namespace]
	if meta, exists := ns.Shards[shardId]; exists {
		fn(&meta)
		ns.Shards[shardId] = meta
		return sc.statusResource.Update(cloned)
	}
	return nil
}

// fenceEnsemble sends NewTerm to all ensemble members and returns the
// head entry IDs for nodes that responded successfully.
func (sc *SplitController) fenceEnsemble(
	shardId int64,
	term int64,
	ensemble []model.Server,
) (map[model.Server]*proto.EntryId, error) {
	type fenceResult struct {
		server model.Server
		entry  *proto.EntryId
		err    error
	}

	ch := make(chan fenceResult, len(ensemble))
	wg := sync.WaitGroup{}

	for _, server := range ensemble {
		pinnedServer := server
		wg.Go(func() {
			res, err := sc.rpcProvider.NewTerm(sc.ctx, pinnedServer, &proto.NewTermRequest{
				Namespace: sc.namespace,
				Shard:     shardId,
				Term:      term,
			})
			var entry *proto.EntryId
			if res != nil {
				entry = res.HeadEntryId
			}
			ch <- fenceResult{server: pinnedServer, entry: entry, err: err}
		})
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	results := make(map[model.Server]*proto.EntryId)
	var lastErr error
	for r := range ch {
		if r.err != nil {
			sc.log.Warn("NewTerm failed for server",
				slog.Int64("shard", shardId),
				slog.Any("server", r.server),
				slog.Any("error", r.err),
			)
			lastErr = r.err
			continue
		}
		results[r.server] = r.entry
	}

	// Require majority
	majority := len(ensemble)/2 + 1
	if len(results) < majority {
		return nil, errors.Wrapf(lastErr, "failed to reach quorum for NewTerm on shard %d (got %d/%d)",
			shardId, len(results), len(ensemble))
	}

	return results, nil
}

// pickLeader chooses the server with the highest term/offset from the
// fencing results.
func (*SplitController) pickLeader(entries map[model.Server]*proto.EntryId) model.Server {
	var best model.Server
	var bestEntry *proto.EntryId

	for server, entry := range entries {
		if bestEntry == nil ||
			entry.Term > bestEntry.Term ||
			(entry.Term == bestEntry.Term && entry.Offset > bestEntry.Offset) {
			best = server
			bestEntry = entry
		}
	}

	return best
}

// waitForChildCommitOffset polls until the child's commitOffset reaches the
// target. Uses the provided context for timeout control (the round-based
// CatchUp algorithm passes a round-scoped context).
func (sc *SplitController) waitForChildCommitOffset(ctx context.Context, childId int64, targetOffset int64) error {
	return backoff.RetryNotify(func() error {
		childMeta := sc.loadShardMeta(childId)
		if childMeta == nil || childMeta.Leader == nil {
			return errors.Errorf("child shard %d has no leader", childId)
		}

		resp, err := sc.rpcProvider.GetStatus(ctx, *childMeta.Leader, &proto.GetStatusRequest{
			Shard: childId,
		})
		if err != nil {
			return err
		}

		if resp.CommitOffset >= targetOffset {
			sc.log.Info("Child reached target commit offset",
				slog.Int64("child-shard", childId),
				slog.Int64("target", targetOffset),
				slog.Int64("commit-offset", resp.CommitOffset),
			)
			return nil
		}

		return errors.Errorf("child %d commit offset %d, target %d", childId, resp.CommitOffset, targetOffset)
	}, oxiatime.NewBackOff(ctx), func(err error, duration time.Duration) {
		sc.log.Debug("Waiting for child commit offset",
			slog.Int64("child-shard", childId),
			slog.Int64("target-offset", targetOffset),
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

// reelectChild fences the child ensemble with a new term and re-elects the
// same leader. This establishes a clean term independent of the parent.
func (sc *SplitController) reelectChild(childId int64) error {
	childMeta := sc.loadShardMeta(childId)
	if childMeta == nil {
		return errors.Errorf("child shard %d not found", childId)
	}
	if childMeta.Leader == nil {
		return errors.Errorf("child shard %d has no leader", childId)
	}

	newTerm := childMeta.Term + 1
	headEntries, err := sc.fenceEnsemble(childId, newTerm, childMeta.Ensemble)
	if err != nil {
		return err
	}

	// Re-elect the same leader (the node already has the data).
	newLeader := *childMeta.Leader

	// Build follower map
	followerMap := make(map[string]*proto.EntryId)
	for server, entry := range headEntries {
		if server != newLeader {
			followerMap[server.Internal] = entry
		}
	}

	_, err = sc.rpcProvider.BecomeLeader(sc.ctx, newLeader, &proto.BecomeLeaderRequest{
		Namespace:         sc.namespace,
		Shard:             childId,
		Term:              newTerm,
		ReplicationFactor: uint32(len(childMeta.Ensemble)),
		FollowerMaps:      followerMap,
	})
	if err != nil {
		return errors.Wrapf(err, "BecomeLeader failed for child %d", childId)
	}

	// Update child metadata
	if err := sc.updateChildMeta(childId, func(meta *model.ShardMetadata) {
		meta.Term = newTerm
		meta.Leader = &newLeader
		meta.Status = model.ShardStatusSteadyState
	}); err != nil {
		return err
	}

	sc.log.Info("Child re-elected in clean term",
		slog.Int64("child-shard", childId),
		slog.Any("leader", newLeader),
		slog.Int64("term", newTerm),
	)

	return nil
}
