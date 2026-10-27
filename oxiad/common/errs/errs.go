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

// Package errs holds the sentinel errors of the Oxia servers, which callers match with errors.Is.
package errs

import "errors"

var (
	ErrCorrupted                              = errors.New("corrupted")
	ErrInvalidConfig                          = errors.New("invalid configuration")
	ErrNotFound                               = errors.New("not found")
	ErrNotImplemented                         = errors.New("not implemented")
	ErrUnauthenticated                        = errors.New("unauthenticated")
	ErrUnavailable                            = errors.New("unavailable")
	ErrUnsupported                            = errors.New("unsupported")
	ErrBackupInvalidID                        = errors.New("invalid backup id")
	ErrBackupInvalidManifest                  = errors.New("invalid manifest")
	ErrBalancerNoCandidates                   = errors.New("no candidates available")
	ErrBalancerUnsatisfiedAntiAffinity        = errors.New("unsatisfied anti-affinity")
	ErrWALEmptyPayload                        = errors.New("empty payload")
	ErrWALEmptySegment                        = errors.New("segment has no entries")
	ErrWALInvalidNextOffset                   = errors.New("invalid next offset in wal")
	ErrWALOffsetOutOfBounds                   = errors.New("offset out of bounds")
	ErrWALReaderClosed                        = errors.New("reader already closed")
	ErrWALSegmentClosed                       = errors.New("segment is closed")
	ErrWALSegmentFull                         = errors.New("current segment is full")
	ErrDBBadVersionId                         = errors.New("bad version id")
	ErrDBInvalidSecondaryIndexKey             = errors.New("failed to parse secondary index key")
	ErrDBInvalidSequenceKey                   = errors.New("sequential key operation found a key that is not part of the sequence")
	ErrDBMissingPartitionKey                  = errors.New("sequential key operation requires partition key")
	ErrDBMissingSequenceDeltas                = errors.New("sequential key operation missing some sequence deltas")
	ErrDBNotificationRecord                   = errors.New("write request reaches a notification record")
	ErrDBNotificationsDisabled                = errors.New("notifications disabled")
	ErrDBReplaced                             = errors.New("the database of the shard was replaced")
	ErrDBSequenceDeltaIsZero                  = errors.New("sequential key operation requires first delta do be > 0")
	ErrDBSequenceOverflow                     = errors.New("sequential key operation overflows the sequence")
	ErrDBWriteRejected                        = errors.New("write request rejected")
	ErrManifestInstanceIDMismatch             = errors.New("instance id mismatch")
	ErrMetaAlreadyExists                      = errors.New("metadata already exists")
	ErrMetaBadVersion                         = errors.New("metadata bad version")
	ErrMetaConflict                           = errors.New("metadata conflict")
	ErrMetaFailedPrecondition                 = errors.New("metadata failed precondition")
	ErrShardChangeEnsembleLosesEntries        = errors.New("change ensemble would lose entries of the removed data servers that may be committed")
	ErrShardChangeEnsembleLosesFeatureSupport = errors.New("change ensemble would remove support for negotiated shard features")
	ErrShardElectionFailed                    = errors.New("the election failed to elect a leader")
	ErrShardFeaturesRenegotiation             = errors.New("the term features must be negotiated again")
	ErrShardFollowerNotCaughtUp               = errors.New("follower not caught up yet")
	ErrShardInvalidChangeEnsemble             = errors.New("invalid change ensemble")
	ErrShardInvalidHeadOffset                 = errors.New("invalid head offset")
	ErrShardNotFollower                       = errors.New("this node is not a follower of the shard")
	ErrShardNotReadyForChangeEnsemble         = errors.New("shard is not ready for change ensemble, please retry later")
	ErrShardTermFeaturesNotReported           = errors.New("the leader doesn't report the features pinned by its term")
	ErrShardTooManyCursors                    = errors.New("too many cursors")
)
