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

package oxia

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrAdminInvalidArgument = errors.New("admin invalid argument")
	ErrAdminNotFound        = errors.New("admin resource not found")
	ErrAdminAlreadyExists   = errors.New("admin resource already exists")
	ErrAdminNotLeader       = errors.New("admin request must be handled by leader")
	ErrAdminUnavailable     = errors.New("admin service unavailable")
	ErrAdminUnauthorized    = errors.New("admin request unauthorized")
	ErrAdminUnimplemented   = errors.New("admin operation not implemented")
)

type adminError struct {
	kind  error
	cause error
}

func (e *adminError) Error() string {
	if e.cause == nil {
		return e.kind.Error()
	}
	return fmt.Sprintf("%s: %v", e.kind, e.cause)
}

func (e *adminError) Unwrap() error {
	return e.cause
}

func (e *adminError) Is(target error) bool {
	return target == e.kind
}

func wrapAdminError(kind error, cause error) error {
	if cause == nil {
		return kind
	}
	return &adminError{
		kind:  kind,
		cause: cause,
	}
}

func mapAdminError(err error) error {
	if err == nil {
		return nil
	}

	switch status.Code(err) {
	case codes.InvalidArgument:
		return wrapAdminError(ErrAdminInvalidArgument, err)
	case codes.NotFound:
		return wrapAdminError(ErrAdminNotFound, err)
	case codes.AlreadyExists:
		return wrapAdminError(ErrAdminAlreadyExists, err)
	case codes.FailedPrecondition:
		return wrapAdminError(ErrAdminNotLeader, err)
	case codes.Unauthenticated, codes.PermissionDenied:
		return wrapAdminError(ErrAdminUnauthorized, err)
	case codes.Unimplemented:
		return wrapAdminError(ErrAdminUnimplemented, err)
	case codes.Unavailable:
		return wrapAdminError(ErrAdminUnavailable, err)
	default:
		return err
	}
}
