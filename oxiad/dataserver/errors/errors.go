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

package errors

import (
	stderrors "errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
)

var (
	ErrInvalidTerm          = stderrors.New("invalid term")
	ErrResourceNotAvailable = stderrors.New("resource not available")
	ErrResourceConflict     = stderrors.New("resource conflict")
	ErrInvalidStatus        = stderrors.New("invalid status")
	ErrNodeIsNotMember      = stderrors.New("node is not a member")
)

// IntoGRPCError converts dataserver-internal errors into public gRPC errors.
func IntoGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case stderrors.Is(err, ErrInvalidTerm):
		return constant.ErrInvalidTerm
	case stderrors.Is(err, ErrNodeIsNotMember):
		return constant.ErrNodeIsNotMember
	case stderrors.Is(err, ErrInvalidStatus):
		return constant.ErrInvalidStatus
	case stderrors.Is(err, ErrResourceConflict):
		return constant.ErrResourceUnavailable
	case stderrors.Is(err, ErrResourceNotAvailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Unknown, err.Error())
	}
}
