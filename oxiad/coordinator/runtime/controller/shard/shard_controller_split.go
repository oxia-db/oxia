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

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
)

const DefaultSplitTimeout = 5 * time.Minute

// DefaultCatchUpRoundTimeout is the maximum time to wait for children to reach a
// snapshot of the parent's commitOffset. If the round times out, re-read
// the parent's commitOffset and try again.
const DefaultCatchUpRoundTimeout = 10 * time.Second

// StatusWriter runs a status write of a split on the goroutine that owns the
// shard status, and returns its error. It returns the error of the context
// instead if the context ends before that goroutine takes the write.
type StatusWriter func(ctx context.Context, write func() error) error

// SplitConfig holds the configuration of a Split.
type SplitConfig struct {
	Namespace string
	ShardId   int64
	// SplitMetadata is the split metadata of the parent shard: its child
	// shards and its split point.
	SplitMetadata *proto.SplitMetadata
	// SplitTimeout is the maximum duration of the split up to the point of no
	// return. If the split does not reach it within this time, it is aborted;
	// past it, the split keeps going until it completes.
	// Zero means use DefaultSplitTimeout.
	SplitTimeout time.Duration
	// CatchUpRoundTimeout is the maximum duration of a round of CatchUp or of
	// Cutover. Zero means use DefaultCatchUpRoundTimeout.
	CatchUpRoundTimeout time.Duration

	Metadata    coordmetadata.Metadata
	RpcProvider rpc.Provider
	// Features reports the features supported by each data server. Optional:
	// when nil, no features are negotiated for the children's clean terms.
	Features FeaturesSupplier
	// StatusWriter runs the status writes of the split. Optional: when nil,
	// the split writes the status itself.
	StatusWriter StatusWriter
}

// Split drives the split of a parent shard into two children through the
// phases persisted in the shard status, like Election drives a leader
// election:
//
//	Bootstrap ──► CatchUp ──► Cutover ──► Finalize ──► done
//	    ▲            │           │
//	    └────────────┴───────────┘ a leader election invalidated the observers
//	timeout before Finalize ──► abort ──► done
//
// The split runs on its own goroutine, and sends its status writes to the
// StatusWriter: the event loop of the parent's shard controller, which also
// writes the status of the leader elections of the parent. Each write checks
// the status it applies to, e.g. the point of no return requires the parent
// term that the split froze. An error runs the current phase again from its
// start, after a backoff, and a restart of the coordinator resumes the split
// from its persisted phase.
//
// The owner of the split calls Start, waits for Done, and calls Stop once,
// which returns the outcome of the split.
type Split struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup
	logger    *slog.Logger

	namespace    string
	shardId      int64
	leftChildId  int64
	rightChildId int64
	splitPoint   uint32
	// deadline bounds the phases before the point of no return
	deadline            time.Time
	catchUpRoundTimeout time.Duration
	done                chan struct{}

	metadata    coordmetadata.Metadata
	rpcProvider rpc.Provider
	features    FeaturesSupplier

	// Owned by the goroutine of the split until it returns, then by Stop
	statusWriter     StatusWriter
	requiredFeatures map[int64][]proto.Feature
	outcome          SplitOutcome
	// ended tells whether the split ended on its own, rather than by Stop
	ended bool
}

// SplitOutcome is how a split ended, as returned by Stop.
type SplitOutcome int

const (
	SplitUnfinished SplitOutcome = iota
	SplitCompleted
	SplitAborted
)

// --- Lifecycle ---

// NewSplit creates a split that is not started.
func NewSplit(cfg SplitConfig) *Split {
	features := cfg.Features
	if features == nil {
		features = NoOpFeaturesSupplier
	}
	sp := &Split{
		namespace:        cfg.Namespace,
		shardId:          cfg.ShardId,
		leftChildId:      cfg.SplitMetadata.GetChildShardIds()[0],
		rightChildId:     cfg.SplitMetadata.GetChildShardIds()[1],
		splitPoint:       cfg.SplitMetadata.GetSplitPoint(),
		metadata:         cfg.Metadata,
		rpcProvider:      cfg.RpcProvider,
		features:         features,
		done:             make(chan struct{}),
		statusWriter:     cfg.StatusWriter,
		requiredFeatures: make(map[int64][]proto.Feature),
		logger: slog.With(
			slog.String("component", "shard-split"),
			slog.String("namespace", cfg.Namespace),
			slog.Int64("parent-shard", cfg.ShardId),
		),
	}
	splitTimeout := cfg.SplitTimeout
	if splitTimeout == 0 {
		splitTimeout = DefaultSplitTimeout
	}
	sp.catchUpRoundTimeout = cfg.CatchUpRoundTimeout
	if sp.catchUpRoundTimeout == 0 {
		sp.catchUpRoundTimeout = DefaultCatchUpRoundTimeout
	}
	sp.ctx, sp.ctxCancel = context.WithCancel(context.Background())
	sp.deadline = time.Now().Add(splitTimeout)
	return sp
}

// Start runs the split from its persisted phase, on its own goroutine.
func (sp *Split) Start() {
	sp.wg.Go(func() {
		defer close(sp.done)
		process.DoWithLabels(
			sp.ctx,
			map[string]string{
				"oxia":      "shard-split",
				"namespace": sp.namespace,
				"parent":    fmt.Sprintf("%d", sp.shardId),
			},
			sp.run,
		)
	})
}

// Done is closed once the split ended: completed, aborted, or stopped because
// a shard of the split is gone. The owner then calls Stop.
func (sp *Split) Done() <-chan struct{} {
	return sp.done
}

// Stop stops the split, waits for its goroutine, and returns its outcome. A
// split that has not passed the point of no return is aborted once: the parent
// must not stay frozen until another coordinator resumes the split. Stop runs
// on the goroutine that owns the shard status, so the abort writes the status
// in place. A split whose abort fails, or that passed the point of no return,
// resumes from its persisted phase when a shard controller starts again.
func (sp *Split) Stop() SplitOutcome {
	sp.ctxCancel()
	sp.wg.Wait()

	if sp.outcome == SplitUnfinished && !sp.ended {
		sp.statusWriter = nil
		if phase, exists, err := sp.currentPhase(); err == nil && exists && phase != proto.SplitPhaseFinalize {
			sp.logger.Warn("Aborting split due to timeout or cancellation")
			if err := sp.abort(); err != nil {
				sp.logger.Warn("Failed to persist the split abort", slog.Any("error", err))
			}
		}
	}

	return sp.outcome
}

func (sp *Split) run() {
	// A split stopped before it ended is aborted by Stop, unless the run
	// already started to abort it
	abortStarted := false
	defer func() { sp.ended = abortStarted || sp.ctx.Err() == nil }()
	phaseCtx, cancel := context.WithDeadline(sp.ctx, sp.deadline)
	defer cancel()
	err := backoff.RetryNotify(func() error {
		return sp.runPhases(phaseCtx)
	}, oxiatime.NewBackOff(phaseCtx), sp.logRetry)

	// Past the point of no return the split can only move forward: retry
	// Finalize until it completes, even past the split timeout. Stopping the
	// split leaves it for the next coordinator to resume.
	var phase proto.SplitPhase
	exists := false
	finalizeErr := backoff.RetryNotify(func() error {
		var loadErr error
		if phase, exists, loadErr = sp.currentPhase(); loadErr != nil {
			return loadErr
		}
		if !exists || phase != proto.SplitPhaseFinalize {
			return nil
		}
		return sp.finalize()
	}, oxiatime.NewBackOff(sp.ctx), sp.logRetry)
	switch {
	case exists && phase == proto.SplitPhaseFinalize:
		if finalizeErr != nil && sp.ctx.Err() == nil {
			// A shard of the split is gone, see updateShardsMetaIf
			sp.logger.Warn("Split stopped, it resumes from its persisted state after a restart",
				slog.Any("error", finalizeErr))
		}
		return
	case finalizeErr != nil:
		// The split stopped before the phase could be loaded
		return
	case !exists:
		return
	}
	if err != nil && phaseCtx.Err() == nil {
		// A shard of the split is gone, see updateShardsMetaIf
		sp.logger.Warn("Split stopped, it resumes from its persisted state after a restart", slog.Any("error", err))
	}

	// A split that timed out is aborted until the abort completes; Stop aborts
	// a stopped one
	if phaseCtx.Err() != nil && sp.ctx.Err() == nil {
		abortStarted = true
		sp.logger.Warn("Aborting split due to timeout or cancellation")
		if err := backoff.RetryNotify(sp.abort, oxiatime.NewBackOff(sp.ctx), sp.logRetry); err != nil &&
			sp.ctx.Err() == nil {
			sp.logger.Warn("Failed to persist the split abort", slog.Any("error", err))
		}
	}
}

func (sp *Split) logRetry(err error, duration time.Duration) {
	sp.logger.Warn(
		"Split state machine step failed, retrying",
		slog.Any("error", err),
		slog.Duration("retry-after", duration),
	)
}

// --- Phases ---

// runPhases runs the phases of the split up to the point of no return. It
// returns nil once the split passed it, or is done.
func (sp *Split) runPhases(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return backoff.Permanent(err)
		}
		phase, exists, err := sp.currentPhase()
		if err != nil {
			return err
		}
		if !exists || phase == proto.SplitPhaseFinalize {
			return nil
		}

		sp.logger.Info("Running split phase", slog.String("phase", phase.String()))
		// GetPhaseOrDefault reads an unknown phase as Bootstrap
		switch phase {
		case proto.SplitPhaseCatchUp:
			err = sp.catchUp(ctx)
		case proto.SplitPhaseCutover:
			err = sp.cutover(ctx)
		default:
			err = sp.bootstrap(ctx)
		}
		if err != nil {
			return err
		}
	}
}

// bootstrap validates preconditions, fences the members of the children's
// ensembles, picks the leader of each child, and adds every member of the
// children's ensembles as an observer follower on the parent leader, so that
// the parent streams snapshots and WAL entries to each of them: a majority of
// the child must hold the child's data before the split passes the point of
// no return (see cutover). A member that the parent can't reach yet gets them
// once it is back. Then it records the parent term and the child leaders that
// the observers follow, and moves to CatchUp.
func (sp *Split) bootstrap(ctx context.Context) error {
	sp.logger.Info("Phase Bootstrap: fencing children and adding as observers")

	parentMeta, err := sp.loadShardMeta(sp.shardId)
	if err != nil {
		return err
	}
	if parentMeta == nil || parentMeta.Leader == nil {
		return errors.New("parent shard has no leader")
	}
	if parentMeta.GetStatusOrDefault() != proto.ShardStatusSteadyState {
		return errors.New("parent shard is not in steady state")
	}

	for _, childId := range sp.childIDs() {
		if err := sp.fenceAndResetChild(ctx, childId, parentMeta.Term); err != nil {
			return err
		}
	}
	// The children were fenced in the parent term that AddFollower carries: if
	// the parent had a new election in the meantime, AddFollower fails with an
	// invalid-term error and Bootstrap is retried from scratch.
	childLeaders := make(map[int64]string)
	parentHashRange := parentMeta.GetInt32HashRange()
	for _, childId := range sp.childIDs() {
		childMeta, err := sp.loadShardMeta(childId)
		if err != nil {
			return err
		}
		if childMeta == nil || childMeta.Leader == nil {
			return errors.Errorf("child shard %d has no leader", childId)
		}
		// The child inherits the features enabled on the parent. Past the point
		// of no return, every member that takes part in its clean term must
		// support them: the child stays without a leader otherwise, and the split
		// can neither complete nor abort (see reelectChild). Report the features
		// that the whole child ensemble supports, a member whose features are not
		// known yet counting as supporting none, so that the parent leader
		// refuses the child if they don't cover the features enabled on the
		// parent: before the parent is frozen, the split can still be aborted.
		// The range of the parent tells the child which of the notifications of
		// the parent it delivers.
		childFeatures := negotiate(sp.features(childMeta.Ensemble), len(childMeta.Ensemble))
		for _, member := range childMeta.Ensemble {
			if _, err := sp.rpcProvider.AddFollower(ctx, parentMeta.Leader, &proto.AddFollowerRequest{
				Namespace:    sp.namespace,
				Shard:        sp.shardId,
				Term:         parentMeta.Term,
				FollowerName: member.GetInternal(),
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
				SplitParentHashRange: &proto.Int32HashRange{
					MinHashInclusive: parentHashRange.GetMin(),
					MaxHashInclusive: parentHashRange.GetMax(),
				},
				FollowerFeatures: &proto.FollowerFeatures{Supported: childFeatures},
			}); err != nil {
				return errors.Wrapf(err, "failed to add member %s of child %d as observer on parent",
					member.GetNameOrDefault(), childId)
			}
		}
		sp.logger.Info("Added the members of the child as observers on parent",
			slog.Int64("child-shard", childId),
			slog.Any("ensemble", childMeta.Ensemble),
		)
		childLeaders[childId] = childMeta.Leader.GetInternal()
	}

	// Record the parent term and child leaders used during bootstrap so CatchUp
	// can detect if a parent or child leader election invalidated the observer
	// cursors.
	if err := sp.updateShardMeta(sp.shardId, func(meta *proto.ShardMetadata) {
		meta.Split.ParentTermAtBootstrap = parentMeta.Term
		meta.Split.ChildLeadersAtBootstrap = childLeaders
	}); err != nil {
		return err
	}
	return sp.updatePhase(proto.SplitPhaseCatchUp)
}

// fenceAndResetChild fences the ensemble of a child in the parent's term, picks
// its leader, and resets its members, unless an earlier attempt did. The child
// is fenced at the parent's term: the observer cursors that stream parent data
// to the members of the child run at the parent's term, and the data server
// converts a member into an observer-follower only when the cursor's term
// matches the child's term (see shardsDirector.GetOrCreateFollower).
func (sp *Split) fenceAndResetChild(ctx context.Context, childId int64, parentTerm int64) error {
	childMeta, err := sp.loadShardMeta(childId)
	if err != nil {
		return err
	}
	if childMeta == nil {
		return errors.Errorf("child shard %d not found", childId)
	}

	childTerm := parentTerm
	if childMeta.Leader != nil && childMeta.Term == childTerm {
		sp.logger.Info("Child already has leader, skipping fence/reset",
			slog.Int64("child-shard", childId),
			slog.Any("leader", childMeta.Leader),
		)
		return nil
	}

	termOptions, err := sp.metadata.GetNamespaceTermOptions(sp.namespace)
	if err != nil {
		return err
	}
	headEntries, _, err := sp.fenceEnsemble(ctx, childId, childTerm, childMeta.Ensemble, termOptions)
	if err != nil {
		return errors.Wrapf(err, "failed to fence child shard %d", childId)
	}
	// The leader of the child is the member with the highest head entry
	var childLeader *proto.DataServerIdentity
	var childLeaderHead *proto.EntryId
	for member, head := range headEntries {
		if childLeaderHead == nil || head.Term > childLeaderHead.Term ||
			(head.Term == childLeaderHead.Term && head.Offset > childLeaderHead.Offset) {
			childLeader, childLeaderHead = member, head
		}
	}

	// The members of the child get their data from the snapshot that the parent
	// sends to its observers, and CatchUp and Cutover measure what they received
	// with the offsets they report: they must only cover data of this term of
	// the parent. A member can have data already, e.g. when a leader election of
	// the parent sent the split back to Bootstrap. It reports it until the
	// installation of the new snapshot wipes it, so the split could pass the
	// point of no return in the meantime, and then lose the data when fencing
	// the parent aborts the installation. Delete the copy of the child on each
	// member, and fence it again with an empty copy: until the split passes the
	// point of no return, the parent has all the data of its children.
	for member := range headEntries {
		_, err := sp.rpcProvider.DeleteShard(ctx, member, &proto.DeleteShardRequest{
			Namespace: sp.namespace,
			Shard:     childId,
			Term:      childTerm,
		})
		if err == nil {
			_, err = sp.rpcProvider.NewTerm(ctx, member, &proto.NewTermRequest{
				Namespace: sp.namespace,
				Shard:     childId,
				Term:      childTerm,
				Options:   termOptions,
			})
		}
		if err != nil {
			return errors.Wrapf(err, "failed to reset member %s of child shard %d",
				member.GetNameOrDefault(), childId)
		}
	}

	if err := sp.updateShardMeta(childId, func(meta *proto.ShardMetadata) {
		meta.Term = childTerm
		meta.Leader = childLeader
		meta.Status = proto.ShardStatusSteadyState
	}); err != nil {
		return err
	}
	sp.logger.Info("Child fenced",
		slog.Int64("child-shard", childId),
		slog.Any("child-leader", childLeader),
		slog.Int64("term", childTerm),
	)
	return nil
}

// catchUp monitors children's commitOffset until they reach the parent's
// current position. Uses a round-based algorithm: snapshot the parent's
// commitOffset, wait up to 10s for both children to reach it. If the round
// expires (parent under heavy write load), re-read and try again.
//
// The commit offset of a member of a child is the offset of the parent's
// entries that it applied: as an observer of the parent, it applies them up to
// the commit offset that the parent advertises (see waitForChildCommitOffset).
func (sp *Split) catchUp(ctx context.Context) error {
	sp.logger.Info("Phase CatchUp: monitoring observer progress")
	for {
		if err := ctx.Err(); err != nil {
			return backoff.Permanent(err)
		}
		if reset, err := sp.resetIfObserversStale(ctx, proto.SplitPhaseCatchUp); err != nil || reset {
			return err
		}

		parentMeta, err := sp.loadShardMeta(sp.shardId)
		if err != nil {
			return err
		}
		if parentMeta == nil || parentMeta.Leader == nil {
			return errors.New("parent has no leader")
		}
		parentStatus, err := sp.rpcProvider.GetStatus(ctx, parentMeta.Leader, &proto.GetStatusRequest{
			Shard: sp.shardId,
		})
		if err != nil {
			return err
		}
		target := parentStatus.CommitOffset
		sp.logger.Info("CatchUp round: waiting for children to reach target",
			slog.Int64("target-commit-offset", target),
		)
		caughtUp, err := sp.waitForChildren(ctx, target, "CatchUp round timed out, retrying")
		if err != nil {
			return err
		}
		if caughtUp {
			sp.logger.Info("All children caught up")
			return sp.updatePhase(proto.SplitPhaseCutover)
		}
	}
}

// resetIfObserversStale sends the split back to Bootstrap if a parent or child
// leader election has invalidated the observer cursors set up during
// Bootstrap, and reports whether it did. It removes the stale observer cursor
// of a child, and unfreezes the parent in Cutover, which may have frozen it.
func (sp *Split) resetIfObserversStale(ctx context.Context, phase proto.SplitPhase) (bool, error) {
	parentMeta, err := sp.loadShardMeta(sp.shardId)
	if err != nil {
		return false, err
	}
	if parentMeta == nil || parentMeta.Split == nil {
		return false, errors.New("parent or split metadata missing")
	}

	// Parent leader election: observer cursors are closed when the old leader
	// is fenced, so they need to be re-added on the new leader. Bootstrap
	// records the parent term before moving to CatchUp, so it is set even when
	// it is 0, the first term of a shard.
	stale := parentMeta.Term != parentMeta.Split.ParentTermAtBootstrap
	if stale {
		sp.logger.Warn("Parent term changed since bootstrap, resetting to Bootstrap",
			slog.Int64("bootstrap-term", parentMeta.Split.ParentTermAtBootstrap),
			slog.Int64("current-term", parentMeta.Term),
		)
	} else if parentMeta.Split.ChildLeadersAtBootstrap != nil && parentMeta.Leader != nil {
		// Child leader election: the observer cursor targets the old (dead)
		// leader. Remove the stale cursor and fall back to Bootstrap to re-add.
		children, err := sp.loadChildrenMeta()
		if err != nil {
			return false, err
		}
		for _, childId := range sp.childIDs() {
			childLeader := children[childId].GetLeader()
			bootstrapLeader, ok := parentMeta.Split.ChildLeadersAtBootstrap[childId]
			if childLeader == nil || !ok || childLeader.GetInternal() == bootstrapLeader {
				continue
			}
			sp.logger.Warn("Child leader changed since bootstrap, removing stale observer and resetting to Bootstrap",
				slog.Int64("child-shard", childId),
				slog.String("old-leader", bootstrapLeader),
				slog.String("new-leader", childLeader.GetInternal()),
			)
			if _, err := sp.rpcProvider.RemoveObserver(ctx, parentMeta.Leader, &proto.RemoveObserverRequest{
				Namespace:    sp.namespace,
				Shard:        sp.shardId,
				Term:         parentMeta.Term,
				FollowerName: bootstrapLeader,
				TargetShard:  childId,
			}); err != nil {
				sp.logger.Warn("Failed to remove the stale observer",
					slog.Int64("child-shard", childId),
					slog.String("old-leader", bootstrapLeader),
					slog.Any("error", err),
				)
			}
			stale = true
			break
		}
	}
	if !stale {
		return false, nil
	}

	if phase == proto.SplitPhaseCutover {
		sp.unfreezeParentBestEffort(ctx, parentMeta.Leader, parentMeta.Term)
	}
	return true, sp.updatePhase(proto.SplitPhaseBootstrap)
}

// cutover freezes the parent — stopping new writes while keeping its observer
// cursors alive — so the children can drain the final tail up to the parent's
// frozen head. Once the leader and a majority of each child have APPLIED that
// tail, the split reaches the point of no return and moves to Finalize, which
// fences the parent (see finalize).
//
// Freezing before fencing closes the gap where fencing the parent destroys the
// observer cursors that feed the children: by the time we fence, the children
// already hold everything up to the parent's final offset.
func (sp *Split) cutover(ctx context.Context) error {
	sp.logger.Info("Phase Cutover: freezing parent and draining its tail")

	// If a parent or child leader election invalidated the observer cursors
	// since bootstrap, rebuild them before cutover. Unfreeze the parent first
	// in case an earlier cutover attempt had frozen it: Bootstrap and CatchUp
	// don't freeze it, and a reset that fails is retried in Cutover.
	if reset, err := sp.resetIfObserversStale(ctx, proto.SplitPhaseCutover); err != nil || reset {
		return err
	}

	parentMeta, err := sp.loadShardMeta(sp.shardId)
	if err != nil {
		return err
	}
	if parentMeta == nil || parentMeta.Leader == nil {
		return errors.New("parent shard has no leader")
	}
	parentLeader := parentMeta.Leader
	parentTerm := parentMeta.Term

	// Freeze the parent. It stops accepting new writes but is NOT fenced, so
	// its observer cursors keep streaming. The head offset stops advancing at
	// the returned value — the final offset for the cutover.
	freezeResp, err := sp.rpcProvider.FreezeShard(ctx, parentLeader, &proto.FreezeShardRequest{
		Namespace: sp.namespace,
		Shard:     sp.shardId,
		Term:      parentTerm,
		Frozen:    true,
	})
	if err != nil {
		return errors.Wrap(err, "failed to freeze parent during cutover")
	}
	parentFinalOffset := freezeResp.HeadOffset
	sp.logger.Info("Parent frozen",
		slog.Int64("term", parentTerm),
		slog.Int64("final-offset", parentFinalOffset),
	)

	// Wait for the leader and a majority of each child to APPLY every entry up
	// to the parent's frozen head (see waitForChildCommitOffset). Receiving them
	// is not enough: a child applies the parent's entries with the split filter
	// only while it observes the parent. Once re-elected (see finalize), it
	// would apply the rest without the filter, and keep the records of the
	// other child. A child applies the entries up to the commit offset the
	// parent advertises: the parent advertises it to the observers even with no
	// new entry to carry it, so the children apply the whole tail once the
	// parent committed it.
	// Observer staleness is checked again after each round, so a parent/child
	// election during the wait falls back to Bootstrap instead of hanging, or
	// instead of passing the point of no return right after the last round.
	for caughtUp := false; !caughtUp; {
		if err := ctx.Err(); err != nil {
			return backoff.Permanent(err)
		}
		var err error
		if caughtUp, err = sp.waitForChildren(ctx, parentFinalOffset, "Cutover round timed out, retrying"); err != nil {
			sp.logger.Info("Cutover round failed, retrying", slog.Any("error", err))
		}
		if reset, err := sp.resetIfObserversStale(ctx, proto.SplitPhaseCutover); err != nil || reset {
			return err
		}
	}
	sp.logger.Info("Children applied parent tail, passing the point of no return",
		slog.Int64("final-offset", parentFinalOffset),
	)

	// The point of no return. The parent is frozen and the children hold
	// everything up to parentFinalOffset, so the split can only complete from
	// here. From now on, neither a retry nor a coordinator restart rolls the
	// split back, and the parent's shard controller no longer elects the
	// parent. A leader election of the parent can still start between the last
	// check of the observer cursors and the point of no return: it closes them,
	// so the split starts over from Bootstrap instead.
	passed, err := sp.persistPointOfNoReturn(parentLeader, parentTerm)
	if err != nil || passed {
		return err
	}
	sp.logger.Warn("Parent leader election started before the point of no return, resetting to Bootstrap",
		slog.Int64("frozen-term", parentTerm),
		slog.Any("frozen-leader", parentLeader),
	)
	sp.unfreezeParentBestEffort(ctx, parentLeader, parentTerm)
	return sp.updatePhase(proto.SplitPhaseBootstrap)
}

// finalize completes a split past the point of no return: it fences the
// parent, re-elects the children in clean terms, and marks the parent for
// deletion. The leader and a majority of each child already hold the child's
// data (see cutover), and the re-election keeps it: the child leader sends its
// followers only the entries that they miss.
// Every step can be repeated, so a failed attempt, or a coordinator restart,
// runs Finalize again from the start.
func (sp *Split) finalize() error {
	parentMeta, err := sp.loadShardMeta(sp.shardId)
	if err != nil {
		return err
	}
	if parentMeta == nil || parentMeta.Split == nil {
		// The split already completed
		return nil
	}

	sp.logger.Info("Phase Finalize: fencing parent and re-electing children")

	// Fence the parent with the term it got at the point of no return. This
	// stops the parent for good and kills the (now fully-drained) observer
	// cursors. The children already hold everything up to the parent's final
	// offset, so no data is lost. Fencing again with the same term, on a retry,
	// succeeds as well. Parent is being torn down (Deleting) after this fence,
	// so its term options are irrelevant — pass nil.
	if _, _, err := sp.fenceEnsemble(sp.ctx, sp.shardId, parentMeta.Term, parentMeta.Ensemble,
		nil); err != nil {
		return errors.Wrap(err, "failed to fence parent")
	}
	sp.logger.Info("Parent fenced", slog.Int64("term", parentMeta.Term))

	for _, childId := range sp.childIDs() {
		if err := sp.reelectChild(childId); err != nil {
			return errors.Wrapf(err, "failed to re-elect child %d leader", childId)
		}
	}

	// The listener, which Stop notifies, deletes the parent and recomputes the
	// shard assignments, so that clients discover the children.
	if err := sp.detachChildren(); err != nil {
		return err
	}
	sp.outcome = SplitCompleted
	return nil
}

// reelectChild fences the ensemble of the child with a new term, and re-elects
// the same leader: this establishes a clean term independent of the parent.
func (sp *Split) reelectChild(childId int64) error {
	childMeta, err := sp.loadShardMeta(childId)
	if err != nil {
		return err
	}
	if childMeta == nil {
		return errors.Errorf("child shard %d not found", childId)
	}
	if childMeta.Leader == nil {
		return errors.Errorf("child shard %d has no leader", childId)
	}

	// The child database inherited the parent's enabled features through the
	// snapshot, so the clean term must carry a negotiated feature set that
	// covers them: the child leader refuses to lead otherwise. The features
	// that an earlier attempt found already enabled on the child are pinned
	// as well (see checkChildFeatures).
	negotiatedFeatures := unionFeatures(
		negotiate(sp.features(childMeta.Ensemble), len(childMeta.Ensemble)),
		sp.requiredFeatures[childId])

	// Record the new term before fencing with it, so that every attempt uses
	// a higher term: if an attempt fails after the leader started leading in
	// its term, the leader rejects both NewTerm and BecomeLeader for that same
	// term. The leader and the status of the child do not change.
	newTerm := childMeta.Term + 1
	if err := sp.updateShardMeta(childId, func(meta *proto.ShardMetadata) {
		meta.Term = newTerm
	}); err != nil {
		return err
	}

	termOptions, err := sp.metadata.GetNamespaceTermOptions(sp.namespace)
	if err != nil {
		return err
	}
	termOptions.Features = negotiatedFeatures
	headEntries, enabledFeatures, err := sp.fenceEnsemble(sp.ctx, childId, newTerm, childMeta.Ensemble,
		termOptions)
	if err != nil {
		return err
	}
	if err := sp.checkChildFeatures(childId, negotiatedFeatures, enabledFeatures, headEntries); err != nil {
		return err
	}

	// Re-elect the same leader (the node already has the data).
	newLeader := childMeta.Leader
	followerMap := make(map[string]*proto.EntryId)
	for server, entry := range headEntries {
		if server.GetNameOrDefault() != newLeader.GetNameOrDefault() {
			followerMap[server.GetInternal()] = entry
		}
	}
	// Not bounded by the rpc timeout: a follower of the child that misses its
	// data, e.g. one that was down while the others observed the parent, only
	// gets it from a snapshot of the leader. A leader with entries left to
	// commit through such followers, e.g. entries it wrote in the term of an
	// earlier attempt, only returns once one of them installed the snapshot,
	// which takes as long as the child is big. An attempt that gave up before
	// then would only make the next one start over: fencing the child in a new
	// term aborts the installation. A leader that goes away still fails the
	// call.
	if _, err := sp.rpcProvider.BecomeLeader(sp.ctx, newLeader, &proto.BecomeLeaderRequest{
		Namespace:         sp.namespace,
		Shard:             childId,
		Term:              newTerm,
		ReplicationFactor: uint32(len(childMeta.Ensemble)),
		FollowerMaps:      followerMap,
		FeaturesSupported: negotiatedFeatures,
	}); err != nil {
		return errors.Wrapf(err, "BecomeLeader failed for child %d", childId)
	}
	sp.logger.Info("Child re-elected in clean term",
		slog.Int64("child-shard", childId),
		slog.Any("leader", newLeader),
		slog.Int64("term", newTerm),
	)
	return nil
}

// abort cleans up a failed/timed-out split that has not yet fenced the parent.
// It removes the observer cursors of the children from the parent leader, if
// Bootstrap added them, unfreezes the parent, if Cutover froze it, then deletes
// the child shards and clears the parent's split metadata; Stop then notifies
// the listener. It returns the errors to retry: the split is kept until the
// parent and the children can be loaded, and the abort is persisted. The
// removal of the observers and the unfreeze are best-effort.
func (sp *Split) abort() error {
	parentMeta, err := sp.loadShardMeta(sp.shardId)
	if err != nil {
		return err
	}
	children, err := sp.loadChildrenMeta()
	if err != nil {
		return err
	}

	// A fresh context: the split may be stopped or past its deadline
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	phase := parentMeta.GetSplit().GetPhaseOrDefault()
	if parentMeta.GetSplit() != nil && parentMeta.Leader != nil && (phase == proto.SplitPhaseBootstrap ||
		phase == proto.SplitPhaseCatchUp || phase == proto.SplitPhaseCutover) {
		for _, childId := range sp.childIDs() {
			if children[childId].GetLeader() == nil {
				continue
			}
			for _, member := range children[childId].Ensemble {
				if _, err := sp.rpcProvider.RemoveObserver(ctx, parentMeta.Leader, &proto.RemoveObserverRequest{
					Namespace:    sp.namespace,
					Shard:        sp.shardId,
					Term:         parentMeta.Term,
					FollowerName: member.GetInternal(),
					TargetShard:  childId,
				}); err != nil {
					sp.logger.Warn("Failed to remove observer during abort",
						slog.Int64("child-shard", childId),
						slog.String("member", member.GetInternal()),
						slog.Any("error", err),
					)
				}
			}
		}
	}
	sp.unfreezeParentBestEffort(ctx, parentMeta.GetLeader(), parentMeta.GetTerm())

	if err := sp.removeSplit(); err != nil {
		return err
	}
	sp.logger.Info("Split aborted, parent restored")
	sp.outcome = SplitAborted
	return nil
}

// --- Persisted status ---

// The functions below read and write the status of the split shards, and keep
// nothing in memory. The status writes go through the status writer, and
// apply to the current status: the shard controller of the parent updates the
// same shards, e.g. with the outcome of a leader election, which a write of an
// earlier copy would revert.

func (sp *Split) currentPhase() (proto.SplitPhase, bool, error) {
	parentMeta, exists, err := sp.metadata.GetShardStatus(sp.namespace, sp.shardId)
	if err != nil {
		return proto.SplitPhaseBootstrap, false, err
	}
	if !exists || parentMeta.UnsafeBorrow().Split == nil {
		return proto.SplitPhaseBootstrap, false, nil
	}
	return parentMeta.UnsafeBorrow().Split.GetPhaseOrDefault(), true, nil
}

// isFinalizingSplitParent reports whether a shard is the parent of a split past
// the point of no return. Such a parent must never be elected again: the split
// can only complete, and the parent is then deleted.
func isFinalizingSplitParent(meta *proto.ShardMetadata) bool {
	split := meta.GetSplit()
	return len(split.GetChildShardIds()) > 0 && split.GetPhaseOrDefault() == proto.SplitPhaseFinalize
}

func (sp *Split) childIDs() []int64 {
	return []int64{sp.leftChildId, sp.rightChildId}
}

// loadShardMeta returns a copy of the metadata of the shard, nil if it doesn't
// exist, or the error of the status load.
func (sp *Split) loadShardMeta(shardId int64) (*proto.ShardMetadata, error) {
	meta, exists, err := sp.metadata.GetShardStatus(sp.namespace, shardId)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil //nolint:nilnil
	}
	return gproto.Clone(meta.UnsafeBorrow()).(*proto.ShardMetadata), nil //nolint:revive
}

// loadChildrenMeta returns copies of the metadata of the child shards, nil for
// a child that doesn't exist.
func (sp *Split) loadChildrenMeta() (map[int64]*proto.ShardMetadata, error) {
	childrenMeta := make(map[int64]*proto.ShardMetadata, 2)
	for _, childId := range sp.childIDs() {
		childMeta, err := sp.loadShardMeta(childId)
		if err != nil {
			return nil, err
		}
		childrenMeta[childId] = childMeta
	}
	return childrenMeta, nil
}

// patchStatus applies modify to the current status of the namespace, with the
// status writer. A failed write is returned, and the phase that made it is
// retried.
func (sp *Split) patchStatus(modify func(shards map[int64]*proto.ShardMetadata) bool) error {
	write := func() error {
		_, exists, err := sp.metadata.GetNamespaceStatus(sp.namespace)
		if err != nil {
			return err
		}
		if !exists {
			sp.logger.Warn("namespace status not found while updating the split")
			return nil
		}
		return sp.metadata.PatchNamespaceStatus(sp.namespace, func(status *proto.NamespaceStatus) bool {
			return modify(status.Shards)
		})
	}
	if sp.statusWriter == nil {
		return write()
	}
	return sp.statusWriter(sp.ctx, write)
}

// updatePhase updates the split phase on both parent and children.
func (sp *Split) updatePhase(newPhase proto.SplitPhase) error {
	return sp.patchStatus(func(shards map[int64]*proto.ShardMetadata) bool {
		changed := false
		for _, shardId := range []int64{sp.shardId, sp.leftChildId, sp.rightChildId} {
			if meta, exists := shards[shardId]; exists && meta.Split != nil {
				meta.Split.Phase = newPhase
				changed = true
			}
		}
		return changed
	})
}

// persistPointOfNoReturn moves the split to Finalize, and records the parent
// without a leader, in the new term finalize fences it with, in a single
// status update. It does so only if the parent still has the leader and the
// term it was frozen with. A leader election of the parent that started since
// then has stored a new term: the leader it elects accepts writes that the
// children never receive, so the split must not complete. A leader election
// makes the same check the other way, in the update that starts it (see
// Election.persistNewTerm). persistPointOfNoReturn reports whether the split
// passed the point of no return.
func (sp *Split) persistPointOfNoReturn(frozenLeader *proto.DataServerIdentity, frozenTerm int64) (bool, error) {
	setFinalizePhase := func(meta *proto.ShardMetadata) {
		if meta.Split != nil {
			meta.Split.Phase = proto.SplitPhaseFinalize
		}
	}
	return sp.updateShardsMetaIf(func(shards map[int64]*proto.ShardMetadata) bool {
		parent := shards[sp.shardId]
		return parent.GetTerm() == frozenTerm &&
			parent.GetLeader().GetNameOrDefault() == frozenLeader.GetNameOrDefault()
	}, map[int64]func(meta *proto.ShardMetadata){
		sp.leftChildId:  setFinalizePhase,
		sp.rightChildId: setFinalizePhase,
		sp.shardId: func(meta *proto.ShardMetadata) {
			setFinalizePhase(meta)
			meta.Term = frozenTerm + 1
			meta.Leader = nil
			meta.Status = proto.ShardStatusElection
		},
	})
}

// detachChildren clears the split metadata from the children and from the
// parent, and marks the parent for deletion: the split is done, and the parent
// shard controller handles the actual deletion. All of it is a single status
// update: the shard assignments can be recomputed at any time, and must show
// either the parent or both children, never a mix.
func (sp *Split) detachChildren() error {
	_, err := sp.updateShardsMetaIf(nil, map[int64]func(meta *proto.ShardMetadata){
		sp.leftChildId: func(meta *proto.ShardMetadata) {
			meta.Split = nil
		},
		sp.rightChildId: func(meta *proto.ShardMetadata) {
			meta.Split = nil
		},
		sp.shardId: func(meta *proto.ShardMetadata) {
			meta.Status = proto.ShardStatusDeleting
			meta.Split = nil
		},
	})
	return err
}

// removeSplit deletes the child shards and clears the parent's split metadata
// in a single status write. The coordinator can stop between two writes, e.g.
// when it crashes, and the next one resumes the split from the stored status:
// a split with a child missing can't complete, and its Cutover keeps the
// parent frozen until the split times out again.
func (sp *Split) removeSplit() error {
	return sp.patchStatus(func(shards map[int64]*proto.ShardMetadata) bool {
		changed := false
		for _, childId := range sp.childIDs() {
			if _, exists := shards[childId]; exists {
				delete(shards, childId)
				changed = true
			}
		}
		if parent, exists := shards[sp.shardId]; exists && parent.Split != nil {
			parent.Split = nil
			changed = true
		}
		return changed
	})
}

// updateShardMeta persists a change to the metadata of a shard, as
// updateShardsMetaIf.
func (sp *Split) updateShardMeta(shardId int64, fn func(meta *proto.ShardMetadata)) error {
	_, err := sp.updateShardsMetaIf(nil, map[int64]func(meta *proto.ShardMetadata){shardId: fn})
	return err
}

// updateShardsMetaIf persists changes to the metadata of several shards in a
// single status update, if precondition, when set, holds for their current
// metadata. It reports whether the changes were applied. A shard that is gone
// returns a permanent error: the split stops, and resumes from its persisted
// state after a restart.
func (sp *Split) updateShardsMetaIf(
	precondition func(shards map[int64]*proto.ShardMetadata) bool,
	updates map[int64]func(meta *proto.ShardMetadata),
) (bool, error) {
	var notFound error
	applied := false
	if err := sp.patchStatus(func(shards map[int64]*proto.ShardMetadata) bool {
		notFound = nil
		applied = false
		for shardId := range updates {
			if _, exists := shards[shardId]; !exists {
				notFound = errors.Errorf("shard %d not found while updating shards metadata", shardId)
				return false
			}
		}
		if precondition != nil && !precondition(shards) {
			return false
		}
		for shardId, fn := range updates {
			fn(shards[shardId])
		}
		applied = true
		return true
	}); err != nil {
		return false, err
	}
	if notFound != nil {
		return false, backoff.Permanent(notFound)
	}
	return applied, nil
}

// --- Data servers ---

// unfreezeParentBestEffort lifts a write-freeze previously placed on the parent
// leader in the given term, so it resumes serving writes. Used when cutover
// falls back to Bootstrap or aborts before fencing. Best-effort: the freeze is
// kept in the memory of the leader, so a new parent term or a restart of the
// leader clears it on its own, and after fencing the parent is gone anyway.
func (sp *Split) unfreezeParentBestEffort(ctx context.Context, leader *proto.DataServerIdentity, term int64) {
	if leader == nil {
		return
	}
	// Not canceled with the split: a stopped or timed-out split unfreezes too
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if _, err := sp.rpcProvider.FreezeShard(ctx, leader, &proto.FreezeShardRequest{
		Namespace: sp.namespace,
		Shard:     sp.shardId,
		Term:      term,
		Frozen:    false,
	}); err != nil {
		sp.logger.Warn("Failed to unfreeze parent (best-effort)", slog.Any("error", err))
	}
}

// waitForChildren waits up to the catch-up round timeout for both children to
// reach the target (see waitForChildCommitOffset). It returns true if both
// reached it, false if the round timed out (the caller runs another round).
func (sp *Split) waitForChildren(ctx context.Context, target int64, timedOut string) (bool, error) {
	roundCtx, roundCancel := context.WithTimeout(ctx, sp.catchUpRoundTimeout)
	defer roundCancel()

	for _, childId := range sp.childIDs() {
		if err := sp.waitForChildCommitOffset(roundCtx, childId, target); err != nil {
			if roundCtx.Err() != nil {
				sp.logger.Info(timedOut,
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

// waitForChildCommitOffset polls until the leader of the child, and a majority
// of the child's ensemble that includes it, applied the parent's entries up to
// the target. The parent is deleted once the split completes: a majority of the
// child must hold the child's data by then, so that the loss of a member, like
// the loss of a member of any other shard, can't lose it. The child leader must
// hold it too, as Finalize elects it again: its followers can't have entries
// that it doesn't have.
func (sp *Split) waitForChildCommitOffset(ctx context.Context, childId int64, targetOffset int64) error {
	return backoff.RetryNotify(func() error {
		childMeta, err := sp.loadShardMeta(childId)
		if err != nil {
			return err
		}
		if childMeta == nil || childMeta.Leader == nil {
			return errors.Errorf("child shard %d has no leader", childId)
		}

		resp, err := sp.rpcProvider.GetStatus(ctx, childMeta.Leader, &proto.GetStatusRequest{
			Shard: childId,
		})
		if err != nil {
			return err
		}
		if resp.CommitOffset < targetOffset {
			return errors.Errorf("child %d leader commit offset %d, target %d", childId, resp.CommitOffset, targetOffset)
		}

		applied := 1
		majority := len(childMeta.Ensemble)/2 + 1
		for _, member := range childMeta.Ensemble {
			if applied == majority {
				break
			}
			if member.GetNameOrDefault() == childMeta.Leader.GetNameOrDefault() {
				continue
			}
			status, err := sp.rpcProvider.GetStatus(ctx, member, &proto.GetStatusRequest{Shard: childId})
			if err == nil && status.CommitOffset >= targetOffset {
				applied++
			}
		}
		if applied < majority {
			return errors.Errorf("%d of the %d members of child %d reached commit offset %d",
				applied, len(childMeta.Ensemble), childId, targetOffset)
		}

		sp.logger.Info("Child reached target commit offset",
			slog.Int64("child-shard", childId),
			slog.Int64("target", targetOffset),
			slog.Int("members", applied),
		)
		return nil
	}, oxiatime.NewBackOff(ctx), func(err error, duration time.Duration) {
		sp.logger.Debug("Waiting for child commit offset",
			slog.Int64("child-shard", childId),
			slog.Int64("target-offset", targetOffset),
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

// fenceEnsemble sends NewTerm to all ensemble members and returns the
// head entry IDs for nodes that responded successfully, and the union of the
// features that they report as already enabled in their database. options
// carries the namespace's term settings (notifications + key sorting) so a
// freshly fenced child inherits them; pass nil when fencing a shard that is
// being torn down (e.g. the parent during Finalize), where the settings are
// irrelevant.
func (sp *Split) fenceEnsemble(
	ctx context.Context,
	shardId int64,
	term int64,
	ensemble []*proto.DataServerIdentity,
	options *proto.NewTermOptions,
) (map[*proto.DataServerIdentity]*proto.EntryId, []proto.Feature, error) {
	type fenceResult struct {
		server   *proto.DataServerIdentity
		response *proto.NewTermResponse
		err      error
	}

	ch := make(chan fenceResult, len(ensemble))
	wg := sync.WaitGroup{}

	for _, server := range ensemble {
		pinnedServer := server
		wg.Go(func() {
			res, err := sp.rpcProvider.NewTerm(ctx, pinnedServer, &proto.NewTermRequest{
				Namespace: sp.namespace,
				Shard:     shardId,
				Term:      term,
				Options:   options,
			})
			ch <- fenceResult{server: pinnedServer, response: res, err: err}
		})
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	results := make(map[*proto.DataServerIdentity]*proto.EntryId)
	var enabledFeatures []proto.Feature
	var lastErr error
	for r := range ch {
		if r.err != nil {
			sp.logger.Warn("NewTerm failed for server",
				slog.Int64("shard", shardId),
				slog.Any("server", r.server),
				slog.Any("error", r.err),
			)
			lastErr = r.err
			continue
		}
		results[r.server] = r.response.GetHeadEntryId()
		enabledFeatures = unionFeatures(enabledFeatures, r.response.GetFeaturesEnabled())
	}

	// Require majority
	majority := len(ensemble)/2 + 1
	if len(results) < majority {
		return nil, nil, errors.Wrapf(lastErr, "failed to reach quorum for NewTerm on shard %d (got %d/%d)",
			shardId, len(results), len(ensemble))
	}

	return results, enabledFeatures, nil
}

// checkChildFeatures validates the features pinned by the clean term of a
// child once its ensemble is fenced, as Election.checkNegotiatedFeatures does
// for a leader election. A member counts as not supporting any feature until
// it completes its handshake with this coordinator, e.g. while it is down
// after a coordinator restart, so the features negotiated over the ensemble
// can miss the ones already enabled on the child.
func (sp *Split) checkChildFeatures(childId int64, negotiated []proto.Feature, enabled []proto.Feature,
	fenced map[*proto.DataServerIdentity]*proto.EntryId) error {
	// The pinned features, and the ones already enabled on the child, must be
	// supported by every member that takes part in the term: an old data
	// server accepts a term that pins features it doesn't support, and would
	// apply the child's entries with different semantics. The child stays
	// without a leader until the offending nodes are replaced.
	participants := make([]*proto.DataServerIdentity, 0, len(fenced))
	for dataServer := range fenced {
		participants = append(participants, dataServer)
	}
	supported := negotiate(sp.features(participants), len(participants))
	if missing := feature.Missing(unionFeatures(negotiated, enabled), supported); len(missing) > 0 {
		return errors.Wrapf(constant.ErrUnsupportedFeatures,
			"the ensemble of child shard %d does not support features %v already enabled on it", childId, missing)
	}

	// The members that did not take part are checked when they join the term,
	// and a feature enabled on the child can never be disabled again: pin the
	// enabled features even if those members' support is unknown, instead of
	// failing every attempt until they are back.
	if missing := feature.Missing(enabled, negotiated); len(missing) > 0 {
		sp.requiredFeatures[childId] = unionFeatures(sp.requiredFeatures[childId], enabled)
		return fmt.Errorf("%w: features %v are already enabled on child shard %d",
			ErrFeaturesRenegotiation, missing, childId)
	}
	return nil
}
