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
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	controllerapi "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"
)

// SplitController drives the shard split state machine through 4 phases
// (Bootstrap → CatchUp → Cutover → Finalize). It runs alongside the parent's
// Controller.
type SplitController struct {
	namespace     string
	parentShardId int64
	leftChildId   int64
	rightChildId  int64
	splitPoint    uint32

	metadata      coordmetadata.Metadata
	rpcProvider   rpc.Provider
	eventListener controllerapi.ShardSplitEventListener

	// ensembleSelector selects server ensembles for new shards.
	ensembleSelector func(namespace string) ([]*proto.DataServerIdentity, error)

	// supportedFeaturesSupplier reports the features supported by each data
	// server, used to negotiate the feature set of the children's clean terms.
	supportedFeaturesSupplier DataServerSupportedFeaturesSupplier

	// ctx ends with Close or with the split timeout, which bounds the phases
	// that can still be aborted. finalizeCtx only ends with Close: Finalize is
	// past the point of no return, and must complete the split however long it
	// takes.
	ctx               context.Context
	ctxCancel         context.CancelFunc
	finalizeCtx       context.Context
	finalizeCtxCancel context.CancelFunc
	wg                sync.WaitGroup
	logger            *slog.Logger
}

const DefaultSplitTimeout = 5 * time.Minute

// SplitControllerConfig holds the configuration needed to create a SplitController.
type SplitControllerConfig struct {
	Namespace        string
	ParentShardId    int64
	Metadata         coordmetadata.Metadata
	RpcProvider      rpc.Provider
	EventListener    controllerapi.ShardSplitEventListener
	EnsembleSelector func(namespace string) ([]*proto.DataServerIdentity, error)

	// SupportedFeaturesSupplier reports the features supported by each data
	// server. Optional: when nil, no features are negotiated for the
	// children's clean terms.
	SupportedFeaturesSupplier DataServerSupportedFeaturesSupplier

	// SplitTimeout is the maximum duration of the split up to the point of no
	// return. If the split does not reach it within this time, it is aborted;
	// past it, the split keeps going until it completes.
	// Zero means use DefaultSplitTimeout.
	SplitTimeout time.Duration
}

// NewSplitController creates a new SplitController and starts it running
// in the background. It will pick up from whatever phase is persisted in
// the cluster status.
func NewSplitController(cfg SplitControllerConfig) *SplitController {
	supportedFeaturesSupplier := cfg.SupportedFeaturesSupplier
	if supportedFeaturesSupplier == nil {
		supportedFeaturesSupplier = NoOpSupportedFeaturesSupplier
	}
	sc := &SplitController{
		namespace:                 cfg.Namespace,
		parentShardId:             cfg.ParentShardId,
		metadata:                  cfg.Metadata,
		rpcProvider:               cfg.RpcProvider,
		eventListener:             cfg.EventListener,
		ensembleSelector:          cfg.EnsembleSelector,
		supportedFeaturesSupplier: supportedFeaturesSupplier,
		logger: slog.With(
			slog.String("component", "shard-split-controller"),
			slog.String("namespace", cfg.Namespace),
			slog.Int64("parent-shard", cfg.ParentShardId),
		),
	}

	splitTimeout := cfg.SplitTimeout
	if splitTimeout == 0 {
		splitTimeout = DefaultSplitTimeout
	}
	sc.ctx, sc.ctxCancel = context.WithTimeout(context.Background(), splitTimeout)
	sc.finalizeCtx, sc.finalizeCtxCancel = context.WithCancel(context.Background())

	// Load the current split metadata from cluster status
	parentMeta, exists := sc.metadata.GetShardStatus(sc.namespace, sc.parentShardId)
	if !exists || parentMeta.UnsafeBorrow().Split == nil {
		sc.logger.Error("Parent shard or split metadata not found")
		return sc
	}

	split := parentMeta.UnsafeBorrow().Split
	sc.leftChildId = split.ChildShardIds[0]
	sc.rightChildId = split.ChildShardIds[1]
	sc.splitPoint = split.SplitPoint

	sc.wg.Go(func() {
		process.DoWithLabels(
			sc.ctx,
			map[string]string{
				"oxia":      "shard-split-controller",
				"namespace": sc.namespace,
				"parent":    fmt.Sprintf("%d", sc.parentShardId),
			},
			sc.run,
		)
	})

	return sc
}

func (sc *SplitController) Close() {
	sc.ctxCancel()
	sc.finalizeCtxCancel()
	sc.wg.Wait()
}

func (sc *SplitController) run() {
	logRetry := func(err error, duration time.Duration) {
		sc.logger.Warn(
			"Split state machine step failed, retrying",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	}
	err := backoff.RetryNotify(func() error {
		return sc.driveStateMachine()
	}, oxiatime.NewBackOff(sc.ctx), logRetry)

	phase, exists := sc.currentPhase()
	if !exists {
		return
	}
	if phase == proto.SplitPhaseFinalize {
		// Past the point of no return the split can only move forward: retry
		// until it completes, even past the split timeout. Closing the
		// controller leaves it for the next coordinator to resume.
		err = backoff.RetryNotify(sc.runFinalize, oxiatime.NewBackOff(sc.finalizeCtx), logRetry)
		if err != nil && sc.finalizeCtx.Err() == nil {
			// The split state could not be persisted, see updateShardMeta
			sc.logger.Warn("Split stopped, it resumes from its persisted state after a restart", slog.Any("error", err))
		}
		return
	}
	if err != nil && sc.ctx.Err() == nil {
		// The split state could not be persisted, see updateShardMeta
		sc.logger.Warn("Split stopped, it resumes from its persisted state after a restart", slog.Any("error", err))
	}

	// If we exited due to timeout/cancellation and the split isn't done,
	// abort and clean up.
	if sc.ctx.Err() != nil {
		sc.abort()
	}
}

func (sc *SplitController) driveStateMachine() error {
	for {
		if err := sc.ctx.Err(); err != nil {
			return backoff.Permanent(err)
		}

		phase, exists := sc.currentPhase()
		if !exists {
			// Split is done or metadata was cleaned up
			return nil
		}
		if phase == proto.SplitPhaseFinalize {
			// Completed by run, without the split timeout
			return nil
		}

		sc.logger.Info("Running split phase", slog.String("phase", phase.String()))

		var err error
		switch phase {
		case proto.SplitPhaseBootstrap:
			err = sc.runBootstrap()
		case proto.SplitPhaseCatchUp:
			err = sc.runCatchUp()
		case proto.SplitPhaseCutover:
			err = sc.runCutover()
		default:
			sc.logger.Error("Unknown split phase", slog.Any("phase", phase))
			return nil
		}

		if err != nil {
			return err
		}
	}
}

func (sc *SplitController) currentPhase() (proto.SplitPhase, bool) {
	parentMeta, exists := sc.metadata.GetShardStatus(sc.namespace, sc.parentShardId)
	if !exists || parentMeta.UnsafeBorrow().Split == nil {
		return proto.SplitPhaseBootstrap, false
	}
	return parentMeta.UnsafeBorrow().Split.GetPhaseOrDefault(), true
}

// isFinalizingSplitParent reports whether a shard is the parent of a split past
// the point of no return. Such a parent must never be elected again: the split
// can only complete, and the parent is then deleted.
func isFinalizingSplitParent(meta *proto.ShardMetadata) bool {
	split := meta.GetSplit()
	return len(split.GetChildShardIds()) > 0 && split.GetPhaseOrDefault() == proto.SplitPhaseFinalize
}

// updatePhase atomically updates the split phase on both parent and children,
// on their current metadata. Failing to persist it stops the split, as in
// updateShardMeta.
func (sc *SplitController) updatePhase(newPhase proto.SplitPhase) error {
	if _, exists := sc.metadata.GetNamespaceStatus(sc.namespace); !exists {
		sc.logger.Warn("namespace status not found while updating split phase",
			slog.String("namespace", sc.namespace),
			slog.String("phase", newPhase.String()))
		return nil
	}
	if err := sc.metadata.UpdateShardStatuses(sc.namespace, func(shards map[int64]*proto.ShardMetadata) bool {
		changed := false
		for _, shardId := range []int64{sc.parentShardId, sc.leftChildId, sc.rightChildId} {
			if meta, exists := shards[shardId]; exists && meta.Split != nil {
				meta.Split.Phase = newPhase
				changed = true
			}
		}
		return changed
	}); err != nil {
		return backoff.Permanent(err)
	}
	return nil
}

// runBootstrap validates preconditions, fences child ensemble members, elects
// child leaders (so they start replicating to their followers immediately),
// and adds children as observer followers on the parent leader.
func (sc *SplitController) runBootstrap() error {
	sc.logger.Info("Phase Bootstrap: fencing children and adding as observers")

	parentMeta := sc.loadParentMeta()
	if parentMeta == nil || parentMeta.Leader == nil {
		return errors.New("parent shard has no leader")
	}
	if parentMeta.GetStatusOrDefault() != proto.ShardStatusSteadyState {
		return errors.New("parent shard is not in steady state")
	}
	parentLeader := parentMeta.Leader
	parentTerm := parentMeta.Term

	// Step 1: Fence and elect each child leader (if not already done).
	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		if err := sc.fenceAndElectChild(childId, parentTerm); err != nil {
			return err
		}
	}

	// Step 2: Add each child leader as an observer on the parent leader,
	// using the same parent term the children were fenced with. If the
	// parent had a new election in the meantime, AddFollower fails with
	// an invalid-term error and Bootstrap is retried from scratch.
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
			childLeaders[childId] = childMeta.Leader.GetInternal()
		}
	}
	if err := sc.updateParentMeta(func(meta *proto.ShardMetadata) {
		meta.Split.ParentTermAtBootstrap = parentTerm
		meta.Split.ChildLeadersAtBootstrap = childLeaders
	}); err != nil {
		return err
	}

	return sc.updatePhase(proto.SplitPhaseCatchUp)
}

// fenceAndElectChild fences a child shard's ensemble and elects a leader.
// The child is fenced at the parent's current term: the observer cursor that
// streams parent data to the child runs at the parent's term, and the data
// server converts the child leader into an observer-follower only when the
// cursor's term matches the child's term (see shardsDirector.GetOrCreateFollower).
// Skipped if the child already has a leader at that term (from a previous
// Bootstrap run).
func (sc *SplitController) fenceAndElectChild(childId int64, parentTerm int64) error {
	childMeta := sc.loadShardMeta(childId)
	if childMeta == nil {
		return errors.Errorf("child shard %d not found", childId)
	}

	if childMeta.Leader != nil && childMeta.Term == parentTerm {
		sc.logger.Info("Child already has leader, skipping fence/elect",
			slog.Int64("child-shard", childId),
			slog.Any("leader", childMeta.Leader),
		)
		return nil
	}

	childTerm := parentTerm
	headEntries, err := sc.fenceEnsemble(sc.ctx, childId, childTerm, childMeta.Ensemble, namespaceTermOptions(sc.metadata, sc.namespace))
	if err != nil {
		return errors.Wrapf(err, "failed to fence child shard %d", childId)
	}

	childLeader := sc.pickLeader(headEntries)

	if err := sc.updateChildMeta(childId, func(meta *proto.ShardMetadata) {
		meta.Term = childTerm
		meta.Leader = childLeader
		meta.Status = proto.ShardStatusSteadyState
	}); err != nil {
		return err
	}

	// Elect the child leader so it replicates to its followers immediately.
	// Without this, only the single child leader node has the data.
	followerMap := make(map[string]*proto.EntryId)
	for server, entry := range headEntries {
		if server.GetNameOrDefault() != childLeader.GetNameOrDefault() {
			followerMap[server.GetInternal()] = entry
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

	sc.logger.Info("Child leader elected",
		slog.Int64("child-shard", childId),
		slog.Any("child-leader", childLeader),
		slog.Int64("term", childTerm),
	)
	return nil
}

// addChildObserver adds a child's leader as an observer follower on the parent
// leader so the parent streams snapshots and WAL entries to it.
func (sc *SplitController) addChildObserver(childId int64, parentLeader *proto.DataServerIdentity, parentTerm int64) error {
	childMeta := sc.loadShardMeta(childId)
	if childMeta == nil || childMeta.Leader == nil {
		return errors.Errorf("child shard %d has no leader", childId)
	}
	childLeader := childMeta.Leader

	// The child leader applies the parent's replicated entries, so the parent
	// leader must be able to validate it supports the shard's features.
	childLeaderFeatures := sc.supportedFeaturesSupplier([]*proto.DataServerIdentity{childLeader})[childLeader.GetNameOrDefault()]

	_, err := sc.rpcProvider.AddFollower(sc.ctx, parentLeader, &proto.AddFollowerRequest{
		Namespace:    sc.namespace,
		Shard:        sc.parentShardId,
		Term:         parentTerm,
		FollowerName: childLeader.GetInternal(),
		FollowerHeadEntryId: &proto.EntryId{
			Term:   -1,
			Offset: -1,
		},
		Observer:    true,
		TargetShard: &childId,
		SplitHashRange: &proto.Int32HashRange{
			MinHashInclusive: childMeta.GetInt32HashRange().GetMin(),
			MaxHashInclusive: childMeta.GetInt32HashRange().GetMax(),
		},
		FollowerFeatures: &proto.FollowerFeatures{Supported: childLeaderFeatures},
	})
	if err != nil {
		return errors.Wrapf(err, "failed to add child %d as observer on parent", childId)
	}

	sc.logger.Info("Added child as observer on parent",
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
	sc.logger.Info("Phase CatchUp: monitoring observer progress")

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
			sc.logger.Info("All children caught up")
			return sc.updatePhase(proto.SplitPhaseCutover)
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
	// is fenced, so they need to be re-added on the new leader. Bootstrap
	// records the parent term before moving to CatchUp, so it is set even when
	// it is 0, the first term of a shard.
	if parentMeta.Term != parentMeta.Split.ParentTermAtBootstrap {
		sc.logger.Warn("Parent term changed since bootstrap, resetting to Bootstrap",
			slog.Int64("bootstrap-term", parentMeta.Split.ParentTermAtBootstrap),
			slog.Int64("current-term", parentMeta.Term),
		)
		if err := sc.updatePhase(proto.SplitPhaseBootstrap); err != nil {
			return false, err
		}
		return true, nil
	}

	// Child leader election: the observer cursor targets the old (dead) leader.
	// Remove the stale cursor and fall back to Bootstrap to re-add.
	if sc.removeStaleChildObservers(parentMeta) {
		if err := sc.updatePhase(proto.SplitPhaseBootstrap); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

// removeStaleChildObservers checks if any child leader changed since Bootstrap.
// If so, removes the stale observer cursor from the parent and returns true.
func (sc *SplitController) removeStaleChildObservers(parentMeta *proto.ShardMetadata) bool {
	if parentMeta.Split.ChildLeadersAtBootstrap == nil || parentMeta.Leader == nil {
		return false
	}
	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		childMeta := sc.loadShardMeta(childId)
		if childMeta == nil || childMeta.Leader == nil {
			continue
		}
		bootstrapLeader, ok := parentMeta.Split.ChildLeadersAtBootstrap[childId]
		if !ok || childMeta.Leader.GetInternal() == bootstrapLeader {
			continue
		}

		sc.logger.Warn("Child leader changed since bootstrap, removing stale observer and resetting to Bootstrap",
			slog.Int64("child-shard", childId),
			slog.String("old-leader", bootstrapLeader),
			slog.String("new-leader", childMeta.Leader.GetInternal()),
		)
		_, _ = sc.rpcProvider.RemoveObserver(sc.ctx, parentMeta.Leader, &proto.RemoveObserverRequest{
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

	parentStatus, err := sc.rpcProvider.GetStatus(sc.ctx, parentMeta.Leader, &proto.GetStatusRequest{
		Shard: sc.parentShardId,
	})
	if err != nil {
		return false, err
	}
	target := parentStatus.CommitOffset

	sc.logger.Info("CatchUp round: waiting for children to reach target",
		slog.Int64("target-commit-offset", target),
	)

	roundCtx, roundCancel := context.WithTimeout(sc.ctx, CatchUpRoundTimeout)
	defer roundCancel()

	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		if err := sc.waitForChildCommitOffset(roundCtx, childId, target); err != nil {
			if roundCtx.Err() != nil {
				sc.logger.Info("CatchUp round timed out, retrying",
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

// runCutover freezes the parent — stopping new writes while keeping its
// observer cursors alive — so the children can drain the final tail up to the
// parent's frozen head. Once the children have RECEIVED that tail (in their
// WALs; their commit is still capped at the parent's advertised commit), the
// split reaches the point of no return and moves to Finalize, which fences the
// parent (see runFinalize).
//
// Freezing before fencing closes the gap where fencing the parent destroys the
// observer cursors that feed the children: by the time we fence, the children
// already hold everything up to the parent's final offset.
func (sc *SplitController) runCutover() error {
	sc.logger.Info("Phase Cutover: freezing parent and draining its tail")

	// If a parent or child leader election invalidated the observer cursors
	// since bootstrap, rebuild them before cutover. Unfreeze the parent first
	// in case an earlier cutover attempt had frozen it.
	if fallback, err := sc.checkObserverCursorsStale(); err != nil {
		return err
	} else if fallback {
		sc.unfreezeParentBestEffort()
		return nil
	}

	parentMeta := sc.loadParentMeta()
	if parentMeta == nil || parentMeta.Leader == nil {
		return errors.New("parent shard has no leader")
	}
	parentLeader := parentMeta.Leader
	parentTerm := parentMeta.Term

	// Step 1: Freeze the parent. It stops accepting new writes but is NOT
	// fenced, so its observer cursors keep streaming. The head offset stops
	// advancing at the returned value — the final offset for the cutover.
	freezeResp, err := sc.rpcProvider.FreezeShard(sc.ctx, parentLeader, &proto.FreezeShardRequest{
		Namespace: sc.namespace,
		Shard:     sc.parentShardId,
		Term:      parentTerm,
		Frozen:    true,
	})
	if err != nil {
		return errors.Wrap(err, "failed to freeze parent during cutover")
	}
	parentFinalOffset := freezeResp.HeadOffset

	sc.logger.Info("Parent frozen",
		slog.Int64("term", parentTerm),
		slog.Int64("final-offset", parentFinalOffset),
	)

	// Step 2: Wait for both children to RECEIVE every entry up to the parent's
	// frozen head. We check the child head offset, not its commit: a child runs
	// as an observer-follower whose commit is capped at the parent's advertised
	// commit, which can never reach the frozen head (no further entries carry an
	// updated commit). The child has the entries in its WAL (head); re-electing
	// it in a clean term (see runFinalize) commits them through the child's own
	// quorum.
	// Re-check observer staleness after each round, so a parent/child election
	// during the wait falls back to Bootstrap instead of hanging, or instead of
	// passing the point of no return right after the last round.
	for {
		if err := sc.ctx.Err(); err != nil {
			return backoff.Permanent(err)
		}
		caughtUp, err := sc.cutoverCatchUpRound(parentFinalOffset)
		if err != nil {
			return err
		}
		if fallback, err := sc.checkObserverCursorsStale(); err != nil {
			return err
		} else if fallback {
			sc.unfreezeParentBestEffort()
			return nil
		}
		if caughtUp {
			break
		}
	}

	sc.logger.Info("Children received parent tail, passing the point of no return",
		slog.Int64("final-offset", parentFinalOffset),
	)

	// Step 3: The point of no return. The parent is frozen and the children
	// hold everything up to parentFinalOffset, so the split can only complete
	// from here. A single status update moves the split to Finalize and
	// records the parent without a leader, in the new term runFinalize fences
	// it with. From now on, neither a retry nor a coordinator restart rolls the
	// split back, and the parent's shard controller no longer elects the parent.
	newParentTerm := parentTerm + 1
	setFinalizePhase := func(meta *proto.ShardMetadata) {
		if meta.Split != nil {
			meta.Split.Phase = proto.SplitPhaseFinalize
		}
	}
	return sc.updateShardsMeta(map[int64]func(meta *proto.ShardMetadata){
		sc.leftChildId:  setFinalizePhase,
		sc.rightChildId: setFinalizePhase,
		sc.parentShardId: func(meta *proto.ShardMetadata) {
			setFinalizePhase(meta)
			meta.Term = newParentTerm
			meta.Leader = nil
			meta.Status = proto.ShardStatusElection
		},
	})
}

// runFinalize completes a split past the point of no return: it fences the
// parent, re-elects the children in clean terms, which commits the parent's
// tail through each child's own quorum, and marks the parent for deletion.
// Every step can be repeated, so a failed attempt, or a coordinator restart,
// runs Finalize again from the start.
func (sc *SplitController) runFinalize() error {
	parentMeta := sc.loadParentMeta()
	if parentMeta == nil || parentMeta.Split == nil {
		// The split already completed
		return nil
	}

	sc.logger.Info("Phase Finalize: fencing parent and re-electing children")

	// Step 1: Fence the parent with the term it got at the point of no return.
	// This stops the parent for good and kills the (now fully-drained)
	// observer cursors. The children already hold everything up to the
	// parent's final offset, so no data is lost. Fencing again with the same
	// term, on a retry, succeeds as well.
	// Parent is being torn down (Deleting) after this fence, so its term
	// options are irrelevant — pass nil.
	if _, err := sc.fenceEnsemble(sc.finalizeCtx, sc.parentShardId, parentMeta.Term, parentMeta.Ensemble, nil); err != nil {
		return errors.Wrap(err, "failed to fence parent")
	}

	sc.logger.Info("Parent fenced", slog.Int64("term", parentMeta.Term))

	// Step 2: Re-elect child leaders in a clean term (independent of parent).
	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		if err := sc.reelectChild(sc.finalizeCtx, childId); err != nil {
			return errors.Wrapf(err, "failed to re-elect child %d leader", childId)
		}
	}

	// Step 3: Clear split metadata from children and mark parent for deletion.
	// Children are now independent shards.
	if err := sc.detachChildren(); err != nil {
		return err
	}

	// Step 4: Notify the coordinator. This triggers the parent shard
	// controller's DeleteShard (which retries indefinitely with backoff)
	// and recomputes shard assignments so clients discover the children.
	sc.eventListener.SplitComplete(sc.parentShardId, sc.leftChildId, sc.rightChildId)

	return nil
}

// detachChildren clears the split metadata from the children and from the
// parent, and marks the parent for deletion: the split controller's job is
// done, and the parent shard controller handles the actual deletion.
// All of it is a single status update: the shard assignments can be recomputed
// at any time, and must show either the parent or both children, never a mix.
func (sc *SplitController) detachChildren() error {
	return sc.updateShardsMeta(map[int64]func(meta *proto.ShardMetadata){
		sc.leftChildId: func(meta *proto.ShardMetadata) {
			meta.Split = nil
		},
		sc.rightChildId: func(meta *proto.ShardMetadata) {
			meta.Split = nil
		},
		sc.parentShardId: func(meta *proto.ShardMetadata) {
			meta.Status = proto.ShardStatusDeleting
			meta.Split = nil
		},
	})
}

// cutoverCatchUpRound waits up to CatchUpRoundTimeout for both children to
// RECEIVE every entry up to the parent's frozen head (head offset, not commit —
// see runCutover). Returns true if both reached it, false if the round timed
// out (the caller retries). Because the parent is frozen, the target is fixed.
func (sc *SplitController) cutoverCatchUpRound(target int64) (bool, error) {
	roundCtx, roundCancel := context.WithTimeout(sc.ctx, CatchUpRoundTimeout)
	defer roundCancel()

	for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
		if err := sc.waitForChildHeadOffset(roundCtx, childId, target); err != nil {
			if roundCtx.Err() != nil {
				sc.logger.Info("Cutover round timed out, retrying",
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

// unfreezeParentBestEffort lifts a write-freeze previously placed on the parent
// leader, so it resumes serving writes. Used when cutover falls back to
// Bootstrap or aborts before fencing. Best-effort: a new parent term clears the
// freeze on its own, and after fencing the parent is gone anyway.
func (sc *SplitController) unfreezeParentBestEffort() {
	parentMeta := sc.loadParentMeta()
	if parentMeta == nil || parentMeta.Leader == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := sc.rpcProvider.FreezeShard(ctx, parentMeta.Leader, &proto.FreezeShardRequest{
		Namespace: sc.namespace,
		Shard:     sc.parentShardId,
		Term:      parentMeta.Term,
		Frozen:    false,
	}); err != nil {
		sc.logger.Warn("Failed to unfreeze parent (best-effort)", slog.Any("error", err))
	}
}

// abort cleans up a failed/timed-out split that has not yet fenced the parent.
// It unfreezes the parent (if cutover had frozen it), removes observer cursors
// from the parent, deletes child shards from status, clears the parent's split
// metadata, and notifies the coordinator.
func (sc *SplitController) abort() {
	sc.logger.Warn("Aborting split due to timeout or cancellation")

	// Use a fresh context since the split context is cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	parentMeta := sc.loadParentMeta()

	// Remove observer cursors from parent leader (best-effort).
	// Only needed if we reached Bootstrap (observers were added).
	if parentMeta != nil && parentMeta.Split != nil && parentMeta.Leader != nil {
		phase := parentMeta.Split.GetPhaseOrDefault()
		if phase == proto.SplitPhaseBootstrap || phase == proto.SplitPhaseCatchUp || phase == proto.SplitPhaseCutover {
			for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
				childMeta := sc.loadShardMeta(childId)
				if childMeta != nil && childMeta.Leader != nil {
					_, err := sc.rpcProvider.RemoveObserver(ctx, parentMeta.Leader, &proto.RemoveObserverRequest{
						Namespace:    sc.namespace,
						Shard:        sc.parentShardId,
						Term:         parentMeta.Term,
						FollowerName: childMeta.Leader.GetInternal(),
						TargetShard:  childId,
					})
					if err != nil {
						sc.logger.Warn("Failed to remove observer during abort",
							slog.Int64("child-shard", childId),
							slog.Any("error", err),
						)
					}
				}
			}
		}
	}

	// Lift any write-freeze placed on the parent during cutover so it resumes
	// serving (best-effort; a new term would clear it anyway).
	sc.unfreezeParentBestEffort()

	// If the coordinator is closing and this cannot be persisted, the split
	// resumes after a restart.
	if err := sc.removeSplit(); err != nil {
		sc.logger.Warn("Failed to persist the split abort", slog.Any("error", err))
		return
	}

	sc.logger.Info("Split aborted, parent restored")

	// Notify coordinator to clean up child controllers and recompute assignments.
	sc.eventListener.SplitAborted(sc.parentShardId, sc.leftChildId, sc.rightChildId)
}

// removeSplit deletes the child shards and clears the parent's split metadata
// in a single status write. The coordinator can stop between two writes, e.g.
// when it crashes, and the next one resumes the split from the stored status:
// a split with a child missing can't complete, and its Cutover keeps the
// parent frozen until the split times out again.
func (sc *SplitController) removeSplit() error {
	if _, exists := sc.metadata.GetNamespaceStatus(sc.namespace); !exists {
		sc.logger.Warn("namespace status not found while removing the split",
			slog.String("namespace", sc.namespace))
		return nil
	}
	return sc.metadata.UpdateShardStatuses(sc.namespace, func(shards map[int64]*proto.ShardMetadata) bool {
		changed := false
		for _, childId := range []int64{sc.leftChildId, sc.rightChildId} {
			if _, exists := shards[childId]; exists {
				delete(shards, childId)
				changed = true
			}
		}
		if parent, exists := shards[sc.parentShardId]; exists && parent.Split != nil {
			parent.Split = nil
			changed = true
		}
		return changed
	})
}

// --- Helper methods ---

func (sc *SplitController) loadParentMeta() *proto.ShardMetadata {
	return sc.loadShardMeta(sc.parentShardId)
}

func (sc *SplitController) loadShardMeta(shardId int64) *proto.ShardMetadata {
	meta, exists := sc.metadata.GetShardStatus(sc.namespace, shardId)
	if !exists {
		return nil
	}
	return gproto.Clone(meta.UnsafeBorrow()).(*proto.ShardMetadata) //nolint:revive
}

func (sc *SplitController) updateParentMeta(fn func(meta *proto.ShardMetadata)) error {
	return sc.updateShardMeta(sc.parentShardId, fn)
}

func (sc *SplitController) updateChildMeta(childId int64, fn func(meta *proto.ShardMetadata)) error {
	return sc.updateShardMeta(childId, fn)
}

// updateShardMeta persists a change to a shard's metadata. A failed write (the
// coordinator is closing, or the shard is gone) returns a permanent error: the
// split must stop rather than retry its phases from a state that was not
// persisted. It resumes from the persisted state after a restart.
func (sc *SplitController) updateShardMeta(shardId int64, fn func(meta *proto.ShardMetadata)) error {
	ns, exists := sc.metadata.GetNamespaceStatus(sc.namespace)
	if !exists {
		sc.logger.Warn("namespace status not found while updating shard metadata",
			slog.String("namespace", sc.namespace),
			slog.Int64("shard", shardId))
		return nil
	}
	meta, exists := ns.UnsafeBorrow().Shards[shardId]
	if !exists {
		sc.logger.Warn("shard metadata not found while updating shard metadata",
			slog.String("namespace", sc.namespace),
			slog.Int64("shard", shardId))
		return nil
	}
	cloned := gproto.Clone(meta).(*proto.ShardMetadata) //nolint:revive
	fn(cloned)
	if err := sc.metadata.UpdateShardStatus(sc.namespace, shardId, cloned); err != nil {
		return backoff.Permanent(err)
	}
	return nil
}

// updateShardsMeta persists changes to the metadata of several shards in a
// single status update, applied to their current metadata: all of them, or
// none if one of the shards is gone. A failed write returns a permanent error,
// as in updateShardMeta.
func (sc *SplitController) updateShardsMeta(updates map[int64]func(meta *proto.ShardMetadata)) error {
	if _, exists := sc.metadata.GetNamespaceStatus(sc.namespace); !exists {
		sc.logger.Warn("namespace status not found while updating shards metadata",
			slog.String("namespace", sc.namespace))
		return nil
	}
	var notFound error
	err := sc.metadata.UpdateShardStatuses(sc.namespace, func(shards map[int64]*proto.ShardMetadata) bool {
		notFound = nil
		for shardId := range updates {
			if _, exists := shards[shardId]; !exists {
				notFound = errors.Errorf("shard %d not found while updating shards metadata", shardId)
				return false
			}
		}
		for shardId, fn := range updates {
			fn(shards[shardId])
		}
		return true
	})
	if err == nil {
		err = notFound
	}
	if err != nil {
		return backoff.Permanent(err)
	}
	return nil
}

// fenceEnsemble sends NewTerm to all ensemble members and returns the
// head entry IDs for nodes that responded successfully. options carries the
// namespace's term settings (notifications + key sorting) so a freshly fenced
// child inherits them; pass nil when fencing a shard that is being torn down
// (e.g. the parent during Finalize), where the settings are irrelevant.
func (sc *SplitController) fenceEnsemble(
	ctx context.Context,
	shardId int64,
	term int64,
	ensemble []*proto.DataServerIdentity,
	options *proto.NewTermOptions,
) (map[*proto.DataServerIdentity]*proto.EntryId, error) {
	type fenceResult struct {
		server *proto.DataServerIdentity
		entry  *proto.EntryId
		err    error
	}

	ch := make(chan fenceResult, len(ensemble))
	wg := sync.WaitGroup{}

	for _, server := range ensemble {
		pinnedServer := server
		wg.Go(func() {
			res, err := sc.rpcProvider.NewTerm(ctx, pinnedServer, &proto.NewTermRequest{
				Namespace: sc.namespace,
				Shard:     shardId,
				Term:      term,
				Options:   options,
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

	results := make(map[*proto.DataServerIdentity]*proto.EntryId)
	var lastErr error
	for r := range ch {
		if r.err != nil {
			sc.logger.Warn("NewTerm failed for server",
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
func (*SplitController) pickLeader(entries map[*proto.DataServerIdentity]*proto.EntryId) *proto.DataServerIdentity {
	var best *proto.DataServerIdentity
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

		resp, err := sc.rpcProvider.GetStatus(ctx, childMeta.Leader, &proto.GetStatusRequest{
			Shard: childId,
		})
		if err != nil {
			return err
		}

		if resp.CommitOffset >= targetOffset {
			sc.logger.Info("Child reached target commit offset",
				slog.Int64("child-shard", childId),
				slog.Int64("target", targetOffset),
				slog.Int64("commit-offset", resp.CommitOffset),
			)
			return nil
		}

		return errors.Errorf("child %d commit offset %d, target %d", childId, resp.CommitOffset, targetOffset)
	}, oxiatime.NewBackOff(ctx), func(err error, duration time.Duration) {
		sc.logger.Debug("Waiting for child commit offset",
			slog.Int64("child-shard", childId),
			slog.Int64("target-offset", targetOffset),
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

// waitForChildHeadOffset polls until the child's head offset reaches the target,
// i.e. the child has received (in its WAL) every entry up to that offset. Used
// during cutover, where a child observer-follower has the entries but its commit
// is capped at the parent's advertised commit (see runCutover).
func (sc *SplitController) waitForChildHeadOffset(ctx context.Context, childId int64, targetOffset int64) error {
	return backoff.RetryNotify(func() error {
		childMeta := sc.loadShardMeta(childId)
		if childMeta == nil || childMeta.Leader == nil {
			return errors.Errorf("child shard %d has no leader", childId)
		}

		resp, err := sc.rpcProvider.GetStatus(ctx, childMeta.Leader, &proto.GetStatusRequest{
			Shard: childId,
		})
		if err != nil {
			return err
		}

		if resp.HeadOffset >= targetOffset {
			sc.logger.Info("Child received entries up to target head offset",
				slog.Int64("child-shard", childId),
				slog.Int64("target", targetOffset),
				slog.Int64("head-offset", resp.HeadOffset),
			)
			return nil
		}

		return errors.Errorf("child %d head offset %d, target %d", childId, resp.HeadOffset, targetOffset)
	}, oxiatime.NewBackOff(ctx), func(err error, duration time.Duration) {
		sc.logger.Debug("Waiting for child head offset",
			slog.Int64("child-shard", childId),
			slog.Int64("target-offset", targetOffset),
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

// reelectChild fences the child ensemble with a new term and re-elects the
// same leader. This establishes a clean term independent of the parent.
func (sc *SplitController) reelectChild(ctx context.Context, childId int64) error {
	childMeta := sc.loadShardMeta(childId)
	if childMeta == nil {
		return errors.Errorf("child shard %d not found", childId)
	}
	if childMeta.Leader == nil {
		return errors.Errorf("child shard %d has no leader", childId)
	}

	// The child database inherited the parent's enabled features through the
	// snapshot, so the clean term must carry a negotiated feature set that
	// covers them: the child leader refuses to lead otherwise.
	negotiatedFeatures := negotiate(sc.supportedFeaturesSupplier(childMeta.Ensemble), len(childMeta.Ensemble))

	// Record the new term before fencing with it, so that every attempt uses
	// a higher term: if an attempt fails after the leader started leading in
	// its term, the leader rejects both NewTerm and BecomeLeader for that same
	// term. The leader and the status of the child do not change.
	newTerm := childMeta.Term + 1
	if err := sc.updateChildMeta(childId, func(meta *proto.ShardMetadata) {
		meta.Term = newTerm
	}); err != nil {
		return err
	}

	termOptions := namespaceTermOptions(sc.metadata, sc.namespace)
	termOptions.Features = negotiatedFeatures
	headEntries, err := sc.fenceEnsemble(ctx, childId, newTerm, childMeta.Ensemble, termOptions)
	if err != nil {
		return err
	}

	// Re-elect the same leader (the node already has the data).
	newLeader := childMeta.Leader

	// Build follower map
	followerMap := make(map[string]*proto.EntryId)
	for server, entry := range headEntries {
		if server.GetNameOrDefault() != newLeader.GetNameOrDefault() {
			followerMap[server.GetInternal()] = entry
		}
	}

	_, err = sc.rpcProvider.BecomeLeader(ctx, newLeader, &proto.BecomeLeaderRequest{
		Namespace:         sc.namespace,
		Shard:             childId,
		Term:              newTerm,
		ReplicationFactor: uint32(len(childMeta.Ensemble)),
		FollowerMaps:      followerMap,
		FeaturesSupported: negotiatedFeatures,
	})
	if err != nil {
		return errors.Wrapf(err, "BecomeLeader failed for child %d", childId)
	}

	sc.logger.Info("Child re-elected in clean term",
		slog.Int64("child-shard", childId),
		slog.Any("leader", newLeader),
		slog.Int64("term", newTerm),
	)

	return nil
}
