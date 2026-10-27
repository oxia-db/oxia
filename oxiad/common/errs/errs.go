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

// Package errs holds the internal errors of the Oxia servers. The errors that reach the clients are in
// common/constant.
package errs

import "errors"

var (
	// The common errors, returned with the detail wrapped, e.g. fmt.Errorf("%w: namespace %q", ErrNotFound, name).
	ErrAlreadyClosed      = errors.New("already closed")
	ErrAlreadyExists      = errors.New("already exists")
	ErrBadVersion         = errors.New("bad version")
	ErrDataCorrupted      = errors.New("data corrupted")
	ErrFailedPrecondition = errors.New("failed precondition")
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrNotFound           = errors.New("not found")
	ErrNotImplemented     = errors.New("not implemented")
	ErrUnauthenticated    = errors.New("unauthenticated")
	ErrUnavailable        = errors.New("unavailable")
	// The special errors, which a caller tells apart to react to that failure on its own.
	ErrEmptyPayload          = errors.New("empty payload")
	ErrEmptySegment          = errors.New("segment has no entries")
	ErrFeaturesRenegotiation = errors.New("the term features must be negotiated again")
	ErrInvalidSequentialPut  = errors.New("invalid sequential put")
	ErrNotificationRecord    = errors.New("write request reaches a notification record")
	ErrSegmentFull           = errors.New("current segment is full")
	ErrWriteRejected         = errors.New("write request rejected")
)
