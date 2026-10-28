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
// common/constant: when a client must tell an internal error apart, the server translates it into a public one
// where it answers the client.
//
// To report a failure, return the common error of its category with the detail wrapped, e.g.
// fmt.Errorf("%w: namespace %q", errs.ErrNotFound, name). If no common error fits, add one.
//
// Add a special error only when a caller must tell the failure apart, to react to it on its own: define it on top
// of the common error of its category, e.g. fmt.Errorf("%w: segment full", ErrFailedPrecondition), and prefix its
// name with its component, e.g. ErrWAL. A caller matching the common error also matches the special errors on top
// of it, so check that none of them reaches such a caller by mistake.
package errs

import (
	"errors"
	"fmt"
)

var (
	// The common errors.
	ErrAlreadyClosed      = errors.New("already closed")
	ErrAlreadyExists      = errors.New("already exists")
	ErrConflict           = errors.New("conflict")
	ErrDataCorrupted      = errors.New("data corrupted")
	ErrFailedPrecondition = errors.New("failed precondition")
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrNotFound           = errors.New("not found")
	ErrNotImplemented     = errors.New("not implemented")
	ErrOffsetOverflow     = errors.New("offset overflow")
	ErrUnauthenticated    = errors.New("unauthenticated")
	ErrUnavailable        = errors.New("unavailable")
	// The special errors.
	ErrWALEmptyPayload        = fmt.Errorf("%w: empty payload", ErrInvalidArgument)
	ErrWALEmptySegment        = fmt.Errorf("%w: empty segment", ErrNotFound)
	ErrWALSegmentFull         = fmt.Errorf("%w: segment full", ErrFailedPrecondition)
	ErrDBBadVersion           = fmt.Errorf("%w: bad version", ErrConflict)
	ErrDBKeyNotFound          = fmt.Errorf("%w: key", ErrNotFound)
	ErrDBInvalidSecondaryKey  = fmt.Errorf("%w: invalid secondary key", ErrInvalidArgument)
	ErrDBInvalidSequentialKey = fmt.Errorf("%w: invalid sequential key", ErrInvalidArgument)
	ErrDBInvalidSystemRecord  = fmt.Errorf("%w: invalid system record", ErrInvalidArgument)
	ErrDBWriteRejected        = fmt.Errorf("%w: write rejected", ErrFailedPrecondition)
	ErrShardFeaturesChanged   = fmt.Errorf("%w: features changed", ErrFailedPrecondition)
	ErrShardNotMember         = fmt.Errorf("%w: not member", ErrNotFound)
	ErrMetaBadVersion         = fmt.Errorf("%w: metadata bad version", ErrConflict)
)
