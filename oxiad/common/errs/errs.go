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
	ErrNotReadyForChangeEnsemble         = errors.New("shard is not ready for change ensemble, please retry later")
	ErrInvalidChangeEnsemble             = errors.New("invalid change ensemble")
	ErrFollowerNotCaughtUp               = errors.New("follower not caught up yet")
	ErrElectionFailed                    = errors.New("the election failed to elect a leader")
	ErrChangeEnsembleLosesFeatureSupport = errors.New(
		"change ensemble would remove support for negotiated shard features")
	ErrChangeEnsembleLosesEntries = errors.New(
		"change ensemble would lose entries of the removed data servers that may be committed")
	// ErrFeaturesRenegotiation makes the election retry with a new term, so
	// that it can pin a different feature set than the one it fenced the
	// ensemble with.
	ErrFeaturesRenegotiation = errors.New("the term features must be negotiated again")
	// ErrTermFeaturesNotReported is returned when the leader of the shard
	// doesn't report the features pinned by its term, because it runs a
	// binary that predates the report.
	ErrTermFeaturesNotReported = errors.New("the leader doesn't report the features pinned by its term")

	ErrUnsatisfiedEnsembleReplicas = errors.New("selector: unsatisfied ensemble replicas")
	ErrUnsatisfiedAntiAffinity     = errors.New("selector: unsatisfied anti-affinity")
	ErrUnsupportedAntiAffinityMode = errors.New("selector: unsupported anti-affinity mode")
	ErrNoFunctioning               = errors.New("selector: no functioning selection")
	ErrMultipleResult              = errors.New("selector: multiple results")
	ErrNoCandidates                = errors.New("selector: no candidates available")

	ErrCoordinatorLeaderUnavailable = errors.New("coordinator leader unavailable")

	ErrMetadataNotInitialized     = errors.New("metadata not initialized")
	ErrMetadataBadVersion         = errors.New("metadata bad version")
	ErrMetadataConflict           = errors.New("metadata conflict")
	ErrMetadataAlreadyExists      = errors.New("metadata already exists")
	ErrMetadataNotFound           = errors.New("metadata not found")
	ErrMetadataFailedPrecondition = errors.New("metadata failed precondition")

	ErrBadVersionId          = errors.New("oxia: bad version id")
	ErrMissingPartitionKey   = errors.New("oxia: sequential key operation requires partition key")
	ErrMissingSequenceDeltas = errors.New("oxia: sequential key operation missing some sequence deltas")
	ErrSequenceDeltaIsZero   = errors.New("oxia: sequential key operation requires first delta do be > 0")
	ErrSequenceOverflow      = errors.New("oxia: sequential key operation overflows the sequence")
	ErrInvalidSequenceKey    = errors.New("oxia: sequential key operation found a key that is not part of the sequence")
	ErrNotificationRecord    = errors.New("oxia: write request reaches a notification record")
	ErrNotificationsDisabled = errors.New("oxia: notifications disabled")
	// ErrWriteRejected marks a write request that ProcessWrite rejected
	// because it can't be applied to the database: the request has no effect,
	// and its log entry is applied.
	ErrWriteRejected = errors.New("oxia: write request rejected")
	// ErrDatabaseReplaced is returned by the WithDatabase of a controller when
	// the database was replaced or deleted since the checkpoint it was given.
	ErrDatabaseReplaced = errors.New("the database of the shard was replaced")
	// ErrInvalidSecondaryIndexKey indicates that a key does not use the persisted
	// secondary index key format.
	ErrInvalidSecondaryIndexKey = errors.New("oxia db: failed to parse secondary index key")

	ErrKeyNotFound = errors.New("oxia: key not found")

	ErrInstanceIDMismatch = errors.New("instance id mismatch")

	ErrEntryNotFound     = errors.New("oxia: entry not found")
	ErrReaderClosed      = errors.New("oxia: reader already closed")
	ErrInvalidNextOffset = errors.New("oxia: invalid next offset in wal")
	ErrSegmentFull       = errors.New("oxia: current segment is full")
	ErrEmptySegment      = errors.New("oxia: segment has no entries")
	ErrSegmentClosed     = errors.New("oxia: segment is closed")

	ErrOffsetOutOfBounds = errors.New("oxia: offset out of bounds")
	ErrEmptyPayload      = errors.New("oxia: empty payload")
	ErrDataCorrupted     = errors.New("oxia: data corrupted")

	ErrNodeIsNotFollower = errors.New("oxia: node is not follower")

	ErrTooManyCursors    = errors.New("too many cursors")
	ErrInvalidHeadOffset = errors.New("invalid head offset")

	ErrMetadataFetchFailed              = errors.New("metadata fetch failed")
	ErrEmptyIssueURL                    = errors.New("empty issue URL")
	ErrEmptyAllowedAudiences            = errors.New("empty allowed audiences")
	ErrUnknownIssuer                    = errors.New("unknown issuer")
	ErrUserNameNotFound                 = errors.New("username not found")
	ErrForbiddenAudience                = errors.New("forbidden audience")
	ErrNoPublicKeysFound                = errors.New("no public keys found in file")
	ErrUnsupportedProvider              = errors.New("unsupported authentication provider")
	ErrUnMatchedAuthenticationParamType = errors.New("unmatched authentication parameter type")
	ErrEmptyToken                       = errors.New("empty token")
	ErrMalformedToken                   = errors.New("malformed token")

	// ErrInvalidID is returned when a backup ID doesn't parse.
	ErrInvalidID = errors.New("backup: invalid backup id")
	// ErrInvalidManifest is returned for a manifest that doesn't describe a
	// backup this code can restore.
	ErrInvalidManifest = errors.New("backup: invalid manifest")

	// ErrBlobNotFound is returned by Get for a key that holds no object.
	ErrBlobNotFound = errors.New("blob: object not found")

	ErrNotImplement = errors.New("not implement")
)
