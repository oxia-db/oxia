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
	ErrCorrupted                = errors.New("corrupted")
	ErrInvalidArgument          = errors.New("invalid argument")
	ErrNotFound                 = errors.New("not found")
	ErrNotImplemented           = errors.New("not implemented")
	ErrUnauthenticated          = errors.New("unauthenticated")
	ErrUnavailable              = errors.New("unavailable")
	ErrAlreadyExists            = errors.New("already exists")
	ErrBadVersion               = errors.New("bad version")
	ErrEmptyPayload             = errors.New("empty payload")
	ErrEmptySegment             = errors.New("segment has no entries")
	ErrFailedPrecondition       = errors.New("failed precondition")
	ErrFeaturesRenegotiation    = errors.New("the term features must be negotiated again")
	ErrFollowerNotCaughtUp      = errors.New("follower not caught up yet")
	ErrInstanceIDMismatch       = errors.New("instance id mismatch")
	ErrInvalidSecondaryIndexKey = errors.New("failed to parse secondary index key")
	ErrInvalidSequentialPut     = errors.New("invalid sequential put")
	ErrNoCandidates             = errors.New("no candidates available")
	ErrNotFollower              = errors.New("this node is not a follower of the shard")
	ErrNotificationRecord       = errors.New("write request reaches a notification record")
	ErrReaderClosed             = errors.New("reader already closed")
	ErrSegmentClosed            = errors.New("segment is closed")
	ErrSegmentFull              = errors.New("current segment is full")
	ErrTermFeaturesNotReported  = errors.New("the leader doesn't report the features pinned by its term")
	ErrWriteRejected            = errors.New("write request rejected")
)
