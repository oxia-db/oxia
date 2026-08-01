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
	"sync/atomic"
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

const DefaultSplitTimeout = 5 * time.Minute

// Splitting owns the complete lifecycle and mutable state of one shard split.
// Like Election, it borrows the controller's external resources while keeping
// operation-specific state and goroutines out of the controller itself.
type Splitting struct {
	logger    *slog.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup
	started   atomic.Bool

	// borrowed resources
	metadataStore         coordmetadata.Metadata
	rpc                   rpc.Provider
	eventListener         controllerapi.ShardSplitEventListener
	splitter              *Splitter
	executeMetadataUpdate func(func())

	// owned state
	namespace  string
	shard      int64
	leftChild  int64
	rightChild int64
	splitPoint uint32
}

func NewSplitting(
	parentCtx context.Context,
	logger *slog.Logger,
	namespace string,
	shard int64,
	metadataStore coordmetadata.Metadata,
	rpcProvider rpc.Provider,
	executeMetadataUpdate func(func()),
	config SplitterConfig,
) *Splitting {
	timeout := config.SplitTimeout
	if timeout == 0 {
		timeout = DefaultSplitTimeout
	}
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	return &Splitting{
		logger:                logger,
		ctx:                   ctx,
		ctxCancel:             cancel,
		metadataStore:         metadataStore,
		rpc:                   rpcProvider,
		eventListener:         config.EventListener,
		splitter:              NewSplitter(namespace, shard, metadataStore, config),
		executeMetadataUpdate: executeMetadataUpdate,
		namespace:             namespace,
		shard:                 shard,
	}
}

// Initialize validates and persists a new split.
func (s *Splitting) Initialize(splitPoint *uint32) (leftChild int64, rightChild int64, err error) {
	return s.splitter.Split(splitPoint)
}

// Start resumes the split from its persisted phase. RPC work and split metadata
// mutations run asynchronously in the split state machine goroutine.
func (s *Splitting) Start() {
	if !s.started.CompareAndSwap(false, true) {
		panic("bug! the splitting has been started")
	}

	parentMeta, exists := s.metadataStore.GetShardStatus(s.namespace, s.shard)
	if !exists || parentMeta.UnsafeBorrow().Split == nil ||
		len(parentMeta.UnsafeBorrow().Split.ChildShardIds) != 2 {
		return
	}

	split := parentMeta.UnsafeBorrow().Split
	s.leftChild = split.ChildShardIds[0]
	s.rightChild = split.ChildShardIds[1]
	s.splitPoint = split.SplitPoint

	s.wg.Go(func() {
		defer s.ctxCancel()
		process.DoWithLabels(
			s.ctx,
			map[string]string{
				"oxia":      "shard-controller-split",
				"namespace": s.namespace,
				"parent":    fmt.Sprintf("%d", s.shard),
			},
			s.runSplitStateMachine,
		)
	})
}

func (s *Splitting) Stop() {
	s.ctxCancel()
	s.wg.Wait()
	s.logger.Info(
		"Stopped shard splitting",
		slog.Int64("left-child", s.leftChild),
		slog.Int64("right-child", s.rightChild),
	)
}

func (s *Splitting) runSplitStateMachine() {
	_ = backoff.RetryNotify(func() error {
		return s.driveStateMachine()
	}, oxiatime.NewBackOff(s.ctx), func(err error, duration time.Duration) {
		s.logger.Warn(
			"Split state machine step failed, retrying",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})

	// If we exited due to timeout/cancellation and the split isn't done,
	// abort and clean up.
	if s.ctx.Err() != nil {
		phase, exists := s.currentPhase()
		if !exists {
			return
		}
		switch phase {
		case proto.SplitPhaseBootstrap, proto.SplitPhaseCatchUp:
			s.abort()
		case proto.SplitPhaseCutover:
			// Cutover is abortable only before the parent is fenced. After the
			// fence (the point of no return) it is forward-only and is resumed
			// from the persisted phase, so we must not roll it back here.
			if !s.parentFenced() {
				s.abort()
			}
		default:
			// No cleanup needed for any other phase.
		}
	}
}

func (s *Splitting) driveStateMachine() error {
	for {
		if err := s.ctx.Err(); err != nil {
			return backoff.Permanent(err)
		}

		phase, exists := s.currentPhase()
		if !exists {
			// Split is done or metadata was cleaned up
			return nil
		}

		s.logger.Info("Running split phase", slog.String("phase", phase.String()))

		var err error
		switch phase {
		case proto.SplitPhaseBootstrap:
			err = s.runBootstrap()
		case proto.SplitPhaseCatchUp:
			err = s.runCatchUp()
		case proto.SplitPhaseCutover:
			err = s.runCutover()
		default:
			s.logger.Error("Unknown split phase", slog.Any("phase", phase))
			return nil
		}

		if err != nil {
			return err
		}
	}
}

func (s *Splitting) currentPhase() (proto.SplitPhase, bool) {
	parentMeta, exists := s.metadataStore.GetShardStatus(s.namespace, s.shard)
	if !exists || parentMeta.UnsafeBorrow().Split == nil {
		return proto.SplitPhaseBootstrap, false
	}
	return parentMeta.UnsafeBorrow().Split.GetPhaseOrDefault(), true
}

// updatePhase atomically updates the split phase on both parent and children.
func (s *Splitting) updatePhase(newPhase proto.SplitPhase) {
	s.executeMetadataUpdate(func() {
		s.metadataStore.UpdateNamespaceStatus(s.namespace, func(ns *proto.NamespaceStatus) bool {
			changed := false
			for _, shardId := range []int64{s.shard, s.leftChild, s.rightChild} {
				meta, exists := ns.Shards[shardId]
				if !exists || meta.Split == nil {
					continue
				}
				meta.Split.Phase = newPhase
				changed = true
			}
			return changed
		})
	})
}

// runBootstrap validates preconditions, fences child ensemble members, elects
// child leaders (so they start replicating to their followers immediately),
// and adds children as observer followers on the parent leader.
func (s *Splitting) runBootstrap() error {
	s.logger.Info("Phase Bootstrap: fencing children and adding as observers")

	parentMeta := s.loadParentMeta()
	if parentMeta == nil || parentMeta.Leader == nil {
		return errors.New("parent shard has no leader")
	}
	if parentMeta.GetStatusOrDefault() != proto.ShardStatusSteadyState {
		return errors.New("parent shard is not in steady state")
	}
	parentLeader := parentMeta.Leader
	parentTerm := parentMeta.Term

	// Step 1: Fence and elect each child leader (if not already done).
	for _, childId := range []int64{s.leftChild, s.rightChild} {
		if err := s.fenceAndElectChild(childId, parentTerm); err != nil {
			return err
		}
	}

	// Step 2: Add each child leader as an observer on the parent leader,
	// using the same parent term the children were fenced with. If the
	// parent had a new election in the meantime, AddFollower fails with
	// an invalid-term error and Bootstrap is retried from scratch.
	for _, childId := range []int64{s.leftChild, s.rightChild} {
		if err := s.addChildObserver(childId, parentLeader, parentTerm); err != nil {
			return err
		}
	}

	// Record the parent term and child leaders used during bootstrap so
	// CatchUp can detect if a parent or child leader election invalidated
	// the observer cursors.
	childLeaders := make(map[int64]string)
	for _, childId := range []int64{s.leftChild, s.rightChild} {
		childMeta := s.loadShardMeta(childId)
		if childMeta != nil && childMeta.Leader != nil {
			childLeaders[childId] = childMeta.Leader.GetInternal()
		}
	}
	s.updateParentMeta(func(meta *proto.ShardMetadata) {
		meta.Split.ParentTermAtBootstrap = parentTerm
		meta.Split.ChildLeadersAtBootstrap = childLeaders
	})

	s.updatePhase(proto.SplitPhaseCatchUp)
	return nil
}

// fenceAndElectChild fences a child shard's ensemble and elects a leader.
// The child is fenced at the parent's current term: the observer cursor that
// streams parent data to the child runs at the parent's term, and the data
// server converts the child leader into an observer-follower only when the
// cursor's term matches the child's term (see shardsDirector.GetOrCreateFollower).
// Skipped if the child already has a leader at that term (from a previous
// Bootstrap run).
func (s *Splitting) fenceAndElectChild(childId int64, parentTerm int64) error {
	childMeta := s.loadShardMeta(childId)
	if childMeta == nil {
		return errors.Errorf("child shard %d not found", childId)
	}

	if childMeta.Leader != nil && childMeta.Term == parentTerm {
		s.logger.Info("Child already has leader, skipping fence/elect",
			slog.Int64("child-shard", childId),
			slog.Any("leader", childMeta.Leader),
		)
		return nil
	}

	childTerm := parentTerm
	headEntries, err := s.fenceEnsemble(childId, childTerm, childMeta.Ensemble, namespaceTermOptions(s.metadataStore, s.namespace))
	if err != nil {
		return errors.Wrapf(err, "failed to fence child shard %d", childId)
	}

	childLeader := s.pickLeader(headEntries)

	s.updateChildMeta(childId, func(meta *proto.ShardMetadata) {
		meta.Term = childTerm
		meta.Leader = childLeader
		meta.Status = proto.ShardStatusSteadyState
	})

	// Elect the child leader so it replicates to its followers immediately.
	// Without this, only the single child leader node has the data.
	followerMap := make(map[string]*proto.EntryId)
	for server, entry := range headEntries {
		if server.GetNameOrDefault() != childLeader.GetNameOrDefault() {
			followerMap[server.GetInternal()] = entry
		}
	}

	_, err = s.rpc.BecomeLeader(s.ctx, childLeader, &proto.BecomeLeaderRequest{
		Namespace:         s.namespace,
		Shard:             childId,
		Term:              childTerm,
		ReplicationFactor: uint32(len(childMeta.Ensemble)),
		FollowerMaps:      followerMap,
	})
	if err != nil {
		return errors.Wrapf(err, "BecomeLeader failed for child %d", childId)
	}

	s.logger.Info("Child leader elected",
		slog.Int64("child-shard", childId),
		slog.Any("child-leader", childLeader),
		slog.Int64("term", childTerm),
	)
	return nil
}

// addChildObserver adds a child's leader as an observer follower on the parent
// leader so the parent streams snapshots and WAL entries to it.
func (s *Splitting) addChildObserver(childId int64, parentLeader *proto.DataServerIdentity, parentTerm int64) error {
	childMeta := s.loadShardMeta(childId)
	if childMeta == nil || childMeta.Leader == nil {
		return errors.Errorf("child shard %d has no leader", childId)
	}
	childLeader := childMeta.Leader

	_, err := s.rpc.AddFollower(s.ctx, parentLeader, &proto.AddFollowerRequest{
		Namespace:    s.namespace,
		Shard:        s.shard,
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
	})
	if err != nil {
		return errors.Wrapf(err, "failed to add child %d as observer on parent", childId)
	}

	s.logger.Info("Added child as observer on parent",
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
func (s *Splitting) runCatchUp() error {
	s.logger.Info("Phase CatchUp: monitoring observer progress")

	for {
		if err := s.ctx.Err(); err != nil {
			return backoff.Permanent(err)
		}

		if fallback, err := s.checkObserverCursorsStale(); err != nil {
			return err
		} else if fallback {
			return nil
		}

		caughtUp, err := s.runCatchUpRound()
		if err != nil {
			return err
		}
		if caughtUp {
			s.logger.Info("All children caught up")
			s.updatePhase(proto.SplitPhaseCutover)
			return nil
		}
	}
}

// checkObserverCursorsStale detects if a parent or child leader election has
// invalidated the observer cursors set up during Bootstrap. Returns
// (true, nil) if the phase was reset to Bootstrap and the caller should return.
func (s *Splitting) checkObserverCursorsStale() (bool, error) {
	parentMeta := s.loadParentMeta()
	if parentMeta == nil || parentMeta.Split == nil {
		return false, errors.New("parent or split metadata missing")
	}

	// Parent leader election: observer cursors are closed when the old leader
	// is fenced, so they need to be re-added on the new leader.
	if parentMeta.Split.ParentTermAtBootstrap > 0 && parentMeta.Term != parentMeta.Split.ParentTermAtBootstrap {
		s.logger.Warn("Parent term changed since bootstrap, resetting to Bootstrap",
			slog.Int64("bootstrap-term", parentMeta.Split.ParentTermAtBootstrap),
			slog.Int64("current-term", parentMeta.Term),
		)
		s.updatePhase(proto.SplitPhaseBootstrap)
		return true, nil
	}

	// Child leader election: the observer cursor targets the old (dead) leader.
	// Remove the stale cursor and fall back to Bootstrap to re-add.
	if s.removeStaleChildObservers(parentMeta) {
		s.updatePhase(proto.SplitPhaseBootstrap)
		return true, nil
	}

	return false, nil
}

// removeStaleChildObservers checks if any child leader changed since Bootstrap.
// If so, removes the stale observer cursor from the parent and returns true.
func (s *Splitting) removeStaleChildObservers(parentMeta *proto.ShardMetadata) bool {
	if parentMeta.Split.ChildLeadersAtBootstrap == nil || parentMeta.Leader == nil {
		return false
	}
	for _, childId := range []int64{s.leftChild, s.rightChild} {
		childMeta := s.loadShardMeta(childId)
		if childMeta == nil || childMeta.Leader == nil {
			continue
		}
		bootstrapLeader, ok := parentMeta.Split.ChildLeadersAtBootstrap[childId]
		if !ok || childMeta.Leader.GetInternal() == bootstrapLeader {
			continue
		}

		s.logger.Warn("Child leader changed since bootstrap, removing stale observer and resetting to Bootstrap",
			slog.Int64("child-shard", childId),
			slog.String("old-leader", bootstrapLeader),
			slog.String("new-leader", childMeta.Leader.GetInternal()),
		)
		_, _ = s.rpc.RemoveObserver(s.ctx, parentMeta.Leader, &proto.RemoveObserverRequest{
			Namespace:    s.namespace,
			Shard:        s.shard,
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
func (s *Splitting) runCatchUpRound() (bool, error) {
	parentMeta := s.loadParentMeta()
	if parentMeta == nil || parentMeta.Leader == nil {
		return false, errors.New("parent has no leader")
	}

	parentStatus, err := s.rpc.GetStatus(s.ctx, parentMeta.Leader, &proto.GetStatusRequest{
		Shard: s.shard,
	})
	if err != nil {
		return false, err
	}
	target := parentStatus.CommitOffset

	s.logger.Info("CatchUp round: waiting for children to reach target",
		slog.Int64("target-commit-offset", target),
	)

	roundCtx, roundCancel := context.WithTimeout(s.ctx, CatchUpRoundTimeout)
	defer roundCancel()

	for _, childId := range []int64{s.leftChild, s.rightChild} {
		if err := s.waitForChildCommitOffset(roundCtx, childId, target); err != nil {
			if roundCtx.Err() != nil {
				s.logger.Info("CatchUp round timed out, retrying",
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

// runCutover completes the split. It first freezes the parent — stopping new
// writes while keeping its observer cursors alive — so the children can drain
// the final tail up to the parent's frozen head. Only once the children have
// RECEIVED that tail (in their WALs; their commit is still capped at the
// parent's advertised commit) does it fence the parent (the point of no
// return). Re-electing the children in clean terms then commits the tail
// through each child's own quorum, and the parent is marked for deletion.
//
// Freezing before fencing closes the gap where fencing the parent destroys the
// observer cursors that feed the children: by the time we fence, the children
// already hold everything up to the parent's final offset.
func (s *Splitting) runCutover() error {
	s.logger.Info("Phase Cutover: freezing parent, draining tail, then fencing")

	// If a parent or child leader election invalidated the observer cursors
	// since bootstrap, rebuild them before cutover. Unfreeze the parent first
	// in case an earlier cutover attempt had frozen it.
	if fallback, err := s.checkObserverCursorsStale(); err != nil {
		return err
	} else if fallback {
		s.unfreezeParentBestEffort()
		return nil
	}

	parentMeta := s.loadParentMeta()
	if parentMeta == nil || parentMeta.Leader == nil {
		return errors.New("parent shard has no leader")
	}
	parentLeader := parentMeta.Leader
	parentTerm := parentMeta.Term

	// Step 1: Freeze the parent. It stops accepting new writes but is NOT
	// fenced, so its observer cursors keep streaming. The head offset stops
	// advancing at the returned value — the final offset for the cutover.
	freezeResp, err := s.rpc.FreezeShard(s.ctx, parentLeader, &proto.FreezeShardRequest{
		Namespace: s.namespace,
		Shard:     s.shard,
		Term:      parentTerm,
		Frozen:    true,
	})
	if err != nil {
		return errors.Wrap(err, "failed to freeze parent during cutover")
	}
	parentFinalOffset := freezeResp.HeadOffset

	s.logger.Info("Parent frozen",
		slog.Int64("term", parentTerm),
		slog.Int64("final-offset", parentFinalOffset),
	)

	// Step 2: Wait for both children to RECEIVE every entry up to the parent's
	// frozen head. We check the child head offset, not its commit: a child runs
	// as an observer-follower whose commit is capped at the parent's advertised
	// commit, which can never reach the frozen head (no further entries carry an
	// updated commit). The child has the entries in its WAL (head); re-electing
	// it below in a clean term commits them through the child's own quorum.
	// Re-check observer staleness each round so a parent/child election during
	// the wait falls back to Bootstrap instead of hanging.
	for {
		if err := s.ctx.Err(); err != nil {
			return backoff.Permanent(err)
		}
		if fallback, err := s.checkObserverCursorsStale(); err != nil {
			return err
		} else if fallback {
			s.unfreezeParentBestEffort()
			return nil
		}
		caughtUp, err := s.cutoverCatchUpRound(parentFinalOffset)
		if err != nil {
			return err
		}
		if caughtUp {
			break
		}
	}

	s.logger.Info("Children received parent tail, fencing parent",
		slog.Int64("final-offset", parentFinalOffset),
	)

	// Step 3: Fence the parent — the point of no return. This stops the parent
	// for good and kills the (now fully-drained) observer cursors. The children
	// already hold everything up to parentFinalOffset, so no data is lost.
	newParentTerm := parentTerm + 1
	// Parent is being torn down (Deleting) after this fence, so its term
	// options are irrelevant — pass nil.
	if _, err := s.fenceEnsemble(s.shard, newParentTerm, parentMeta.Ensemble, nil); err != nil {
		return errors.Wrap(err, "failed to fence parent during cutover")
	}

	s.updateParentMeta(func(meta *proto.ShardMetadata) {
		meta.Term = newParentTerm
		meta.Leader = nil
		meta.Status = proto.ShardStatusElection
	})

	s.logger.Info("Parent fenced", slog.Int64("new-term", newParentTerm))

	// Step 4: Re-elect child leaders in a clean term (independent of parent).
	for _, childId := range []int64{s.leftChild, s.rightChild} {
		if err := s.reelectChild(childId); err != nil {
			return errors.Wrapf(err, "failed to re-elect child %d leader", childId)
		}
	}

	// Step 5: Clear split metadata from children and mark parent for deletion.
	// Children are now independent shards.
	for _, childId := range []int64{s.leftChild, s.rightChild} {
		s.updateChildMeta(childId, func(meta *proto.ShardMetadata) {
			meta.Split = nil
		})
	}

	s.updateParentMeta(func(meta *proto.ShardMetadata) {
		meta.Status = proto.ShardStatusDeleting
	})

	// Clear split metadata from parent — the split state machine is done.
	// The parent shard controller handles the actual deletion.
	s.updateParentMeta(func(meta *proto.ShardMetadata) {
		meta.Split = nil
	})

	// Step 6: Notify the coordinator. This triggers the parent shard
	// controller's DeleteShard (which retries indefinitely with backoff)
	// and recomputes shard assignments so clients discover the children.
	s.eventListener.SplitComplete(s.shard, s.leftChild, s.rightChild)

	return nil
}

// cutoverCatchUpRound waits up to CatchUpRoundTimeout for both children to
// RECEIVE every entry up to the parent's frozen head (head offset, not commit —
// see runCutover). Returns true if both reached it, false if the round timed
// out (the caller retries). Because the parent is frozen, the target is fixed.
func (s *Splitting) cutoverCatchUpRound(target int64) (bool, error) {
	roundCtx, roundCancel := context.WithTimeout(s.ctx, CatchUpRoundTimeout)
	defer roundCancel()

	for _, childId := range []int64{s.leftChild, s.rightChild} {
		if err := s.waitForChildHeadOffset(roundCtx, childId, target); err != nil {
			if roundCtx.Err() != nil {
				s.logger.Info("Cutover round timed out, retrying",
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
func (s *Splitting) unfreezeParentBestEffort() {
	parentMeta := s.loadParentMeta()
	if parentMeta == nil || parentMeta.Leader == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.rpc.FreezeShard(ctx, parentMeta.Leader, &proto.FreezeShardRequest{
		Namespace: s.namespace,
		Shard:     s.shard,
		Term:      parentMeta.Term,
		Frozen:    false,
	}); err != nil {
		s.logger.Warn("Failed to unfreeze parent (best-effort)", slog.Any("error", err))
	}
}

// parentFenced reports whether the parent has already been fenced during
// cutover — the point of no return — detected by its term having advanced past
// the term recorded at bootstrap. Used to decide whether a timed-out cutover is
// still safe to abort (pre-fence) or must be resumed forward (post-fence).
func (s *Splitting) parentFenced() bool {
	parentMeta := s.loadParentMeta()
	if parentMeta == nil || parentMeta.Split == nil {
		// Split metadata cleared => cutover already finished.
		return true
	}
	return parentMeta.Term > parentMeta.Split.ParentTermAtBootstrap
}

// abort cleans up a failed/timed-out split that has not yet fenced the parent.
// It unfreezes the parent (if cutover had frozen it), removes observer cursors
// from the parent, deletes child shards from status, clears the parent's split
// metadata, and notifies the coordinator.
func (s *Splitting) abort() {
	s.logger.Warn("Aborting split due to timeout or cancellation")

	// Use a fresh context since the split context is cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	parentMeta := s.loadParentMeta()

	// Remove observer cursors from parent leader (best-effort).
	// Only needed if we reached Bootstrap (observers were added).
	if parentMeta != nil && parentMeta.Split != nil && parentMeta.Leader != nil {
		phase := parentMeta.Split.GetPhaseOrDefault()
		if phase == proto.SplitPhaseBootstrap || phase == proto.SplitPhaseCatchUp || phase == proto.SplitPhaseCutover {
			for _, childId := range []int64{s.leftChild, s.rightChild} {
				childMeta := s.loadShardMeta(childId)
				if childMeta != nil && childMeta.Leader != nil {
					_, err := s.rpc.RemoveObserver(ctx, parentMeta.Leader, &proto.RemoveObserverRequest{
						Namespace:    s.namespace,
						Shard:        s.shard,
						Term:         parentMeta.Term,
						FollowerName: childMeta.Leader.GetInternal(),
						TargetShard:  childId,
					})
					if err != nil {
						s.logger.Warn("Failed to remove observer during abort",
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
	s.unfreezeParentBestEffort()

	// Delete child shards from status.
	s.executeMetadataUpdate(func() {
		for _, childId := range []int64{s.leftChild, s.rightChild} {
			s.metadataStore.DeleteShardStatus(s.namespace, childId)
		}
	})

	// Clear parent split metadata.
	s.updateParentMeta(func(meta *proto.ShardMetadata) {
		meta.Split = nil
	})

	s.logger.Info("Split aborted, parent restored")

	// Notify coordinator to clean up child controllers and recompute assignments.
	s.eventListener.SplitAborted(s.shard, s.leftChild, s.rightChild)
}

// --- Helper methods ---

func (s *Splitting) loadParentMeta() *proto.ShardMetadata {
	return s.loadShardMeta(s.shard)
}

func (s *Splitting) loadShardMeta(shardId int64) *proto.ShardMetadata {
	meta, exists := s.metadataStore.GetShardStatus(s.namespace, shardId)
	if !exists {
		return nil
	}
	return gproto.Clone(meta.UnsafeBorrow()).(*proto.ShardMetadata) //nolint:revive
}

func (s *Splitting) updateParentMeta(fn func(meta *proto.ShardMetadata)) {
	s.updateShardMeta(s.shard, fn)
}

func (s *Splitting) updateChildMeta(childId int64, fn func(meta *proto.ShardMetadata)) {
	s.updateShardMeta(childId, fn)
}

func (s *Splitting) updateShardMeta(shardId int64, fn func(meta *proto.ShardMetadata)) {
	s.executeMetadataUpdate(func() {
		ns, exists := s.metadataStore.GetNamespaceStatus(s.namespace)
		if !exists {
			s.logger.Warn("namespace status not found while updating shard metadata",
				slog.String("namespace", s.namespace),
				slog.Int64("shard", shardId))
			return
		}
		meta, exists := ns.UnsafeBorrow().Shards[shardId]
		if !exists {
			s.logger.Warn("shard metadata not found while updating shard metadata",
				slog.String("namespace", s.namespace),
				slog.Int64("shard", shardId))
			return
		}
		cloned := gproto.Clone(meta).(*proto.ShardMetadata) //nolint:revive
		fn(cloned)
		s.metadataStore.UpdateShardStatus(s.namespace, shardId, cloned)
	})
}

// fenceEnsemble sends NewTerm to all ensemble members and returns the
// head entry IDs for nodes that responded successfully. options carries the
// namespace's term settings (notifications + key sorting) so a freshly fenced
// child inherits them; pass nil when fencing a shard that is being torn down
// (e.g. the parent during cutover), where the settings are irrelevant.
func (s *Splitting) fenceEnsemble(
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
			res, err := s.rpc.NewTerm(s.ctx, pinnedServer, &proto.NewTermRequest{
				Namespace: s.namespace,
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
			s.logger.Warn("NewTerm failed for server",
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
func (*Splitting) pickLeader(entries map[*proto.DataServerIdentity]*proto.EntryId) *proto.DataServerIdentity {
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
func (s *Splitting) waitForChildCommitOffset(ctx context.Context, childId int64, targetOffset int64) error {
	return backoff.RetryNotify(func() error {
		childMeta := s.loadShardMeta(childId)
		if childMeta == nil || childMeta.Leader == nil {
			return errors.Errorf("child shard %d has no leader", childId)
		}

		resp, err := s.rpc.GetStatus(ctx, childMeta.Leader, &proto.GetStatusRequest{
			Shard: childId,
		})
		if err != nil {
			return err
		}

		if resp.CommitOffset >= targetOffset {
			s.logger.Info("Child reached target commit offset",
				slog.Int64("child-shard", childId),
				slog.Int64("target", targetOffset),
				slog.Int64("commit-offset", resp.CommitOffset),
			)
			return nil
		}

		return errors.Errorf("child %d commit offset %d, target %d", childId, resp.CommitOffset, targetOffset)
	}, oxiatime.NewBackOff(ctx), func(err error, duration time.Duration) {
		s.logger.Debug("Waiting for child commit offset",
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
func (s *Splitting) waitForChildHeadOffset(ctx context.Context, childId int64, targetOffset int64) error {
	return backoff.RetryNotify(func() error {
		childMeta := s.loadShardMeta(childId)
		if childMeta == nil || childMeta.Leader == nil {
			return errors.Errorf("child shard %d has no leader", childId)
		}

		resp, err := s.rpc.GetStatus(ctx, childMeta.Leader, &proto.GetStatusRequest{
			Shard: childId,
		})
		if err != nil {
			return err
		}

		if resp.HeadOffset >= targetOffset {
			s.logger.Info("Child received entries up to target head offset",
				slog.Int64("child-shard", childId),
				slog.Int64("target", targetOffset),
				slog.Int64("head-offset", resp.HeadOffset),
			)
			return nil
		}

		return errors.Errorf("child %d head offset %d, target %d", childId, resp.HeadOffset, targetOffset)
	}, oxiatime.NewBackOff(ctx), func(err error, duration time.Duration) {
		s.logger.Debug("Waiting for child head offset",
			slog.Int64("child-shard", childId),
			slog.Int64("target-offset", targetOffset),
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

// reelectChild fences the child ensemble with a new term and re-elects the
// same leader. This establishes a clean term independent of the parent.
func (s *Splitting) reelectChild(childId int64) error {
	childMeta := s.loadShardMeta(childId)
	if childMeta == nil {
		return errors.Errorf("child shard %d not found", childId)
	}
	if childMeta.Leader == nil {
		return errors.Errorf("child shard %d has no leader", childId)
	}

	newTerm := childMeta.Term + 1
	headEntries, err := s.fenceEnsemble(childId, newTerm, childMeta.Ensemble, namespaceTermOptions(s.metadataStore, s.namespace))
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

	_, err = s.rpc.BecomeLeader(s.ctx, newLeader, &proto.BecomeLeaderRequest{
		Namespace:         s.namespace,
		Shard:             childId,
		Term:              newTerm,
		ReplicationFactor: uint32(len(childMeta.Ensemble)),
		FollowerMaps:      followerMap,
	})
	if err != nil {
		return errors.Wrapf(err, "BecomeLeader failed for child %d", childId)
	}

	// Update child metadata
	s.updateChildMeta(childId, func(meta *proto.ShardMetadata) {
		meta.Term = newTerm
		meta.Leader = newLeader
		meta.Status = proto.ShardStatusSteadyState
	})

	s.logger.Info("Child re-elected in clean term",
		slog.Int64("child-shard", childId),
		slog.Any("leader", newLeader),
		slog.Int64("term", newTerm),
	)

	return nil
}
