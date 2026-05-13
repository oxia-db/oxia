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

package constant

import (
	"errors"
	"io"
	"strconv"

	errdetails "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const OxiaErrorDomain = "oxia.io"

const (
	ErrorMetadataShard  = "shard"
	ErrorMetadataLeader = "leader"
)

type ErrorMetadata map[string]string

func (m ErrorMetadata) GetLeaderHint() (int64, string, bool) {
	leader := m[ErrorMetadataLeader]
	if leader == "" {
		return 0, "", false
	}
	shard, err := strconv.ParseInt(m[ErrorMetadataShard], 10, 64)
	if err != nil {
		return 0, "", false
	}
	return shard, leader, true
}

const (
	ReasonAborted                 string = "ABORTED"
	ReasonInvalidSessionTimeout   string = "INVALID_SESSION_TIMEOUT"
	ReasonSessionNotFound         string = "SESSION_NOT_FOUND"
	ReasonNamespaceNotFound       string = "NAMESPACE_NOT_FOUND"
	ReasonInvalidTerm             string = "INVALID_TERM"
	ReasonInvalidStatus           string = "INVALID_STATUS"
	ReasonNotificationsNotEnabled string = "NOTIFICATIONS_NOT_ENABLED"
	ReasonNodeIsNotMember         string = "NODE_IS_NOT_MEMBER"
	ReasonNodeIsNotLeader         string = "NODE_IS_NOT_LEADER"
	ReasonNotInitialized          string = "NOT_INITIALIZED"
	ReasonResourceUnavailable     string = "RESOURCE_UNAVAILABLE"
)

var (
	ErrAborted                 = errors.New("oxia: operation was aborted")
	ErrInvalidSessionTimeout   = errors.New("oxia: invalid session timeout")
	ErrSessionNotFound         = errors.New("oxia: session not found")
	ErrNamespaceNotFound       = errors.New("oxia: namespace not found")
	ErrInvalidTerm             = errors.New("oxia: invalid term")
	ErrInvalidStatus           = errors.New("oxia: invalid status")
	ErrNotificationsNotEnabled = errors.New("oxia: notifications not enabled on namespace")
	ErrNodeIsNotMember         = errors.New("oxia: node is not a member")
	ErrNodeIsNotLeader         = errors.New("oxia: node is not leader")
	ErrNotInitialized          = errors.New("oxia: server not initialized yet")
	ErrResourceUnavailable     = errors.New("oxia: resource unavailable")
)

type GrpcStatusOption func(ErrorMetadata)

func WithLeaderHint(shard int64, leader string) GrpcStatusOption {
	return func(metadata ErrorMetadata) {
		if leader == "" {
			return
		}
		metadata[ErrorMetadataShard] = strconv.FormatInt(shard, 10)
		metadata[ErrorMetadataLeader] = leader
	}
}

func IntoGrpcStatus(err error, opts ...GrpcStatusOption) *status.Status {
	if err == nil {
		return status.New(codes.OK, "")
	}

	metadata := make(ErrorMetadata)
	for _, opt := range opts {
		if opt != nil {
			opt(metadata)
		}
	}
	if len(metadata) == 0 {
		metadata = nil
	}
	message := err.Error()
	if st, ok := status.FromError(err); ok {
		message = st.Message()
	}
	statusWithErrorInfo := func(code codes.Code, reason string) *status.Status {
		st := status.New(code, message)
		stWithDetails, detailsErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason:   reason,
			Domain:   OxiaErrorDomain,
			Metadata: metadata,
		})
		if detailsErr != nil {
			return st
		}
		return stWithDetails
	}
	if st, ok := status.FromError(err); ok {
		switch st.Message() {
		case ErrAborted.Error():
			return statusWithErrorInfo(st.Code(), ReasonAborted)
		case ErrInvalidSessionTimeout.Error():
			return statusWithErrorInfo(st.Code(), ReasonInvalidSessionTimeout)
		case ErrSessionNotFound.Error():
			return statusWithErrorInfo(st.Code(), ReasonSessionNotFound)
		case ErrNamespaceNotFound.Error():
			return statusWithErrorInfo(st.Code(), ReasonNamespaceNotFound)
		case ErrInvalidTerm.Error():
			return statusWithErrorInfo(st.Code(), ReasonInvalidTerm)
		case ErrInvalidStatus.Error():
			return statusWithErrorInfo(st.Code(), ReasonInvalidStatus)
		case ErrNotificationsNotEnabled.Error():
			return statusWithErrorInfo(st.Code(), ReasonNotificationsNotEnabled)
		case ErrNodeIsNotMember.Error():
			return statusWithErrorInfo(st.Code(), ReasonNodeIsNotMember)
		case ErrNodeIsNotLeader.Error():
			return statusWithErrorInfo(st.Code(), ReasonNodeIsNotLeader)
		case ErrNotInitialized.Error():
			return statusWithErrorInfo(st.Code(), ReasonNotInitialized)
		case ErrResourceUnavailable.Error():
			return statusWithErrorInfo(st.Code(), ReasonResourceUnavailable)
		default:
			return st
		}
	}

	switch {
	case errors.Is(err, ErrAborted):
		return statusWithErrorInfo(codes.Aborted, ReasonAborted)
	case errors.Is(err, ErrInvalidSessionTimeout):
		return statusWithErrorInfo(codes.InvalidArgument, ReasonInvalidSessionTimeout)
	case errors.Is(err, ErrSessionNotFound):
		return statusWithErrorInfo(codes.NotFound, ReasonSessionNotFound)
	case errors.Is(err, ErrNamespaceNotFound):
		return statusWithErrorInfo(codes.NotFound, ReasonNamespaceNotFound)
	case errors.Is(err, ErrInvalidTerm):
		return statusWithErrorInfo(codes.FailedPrecondition, ReasonInvalidTerm)
	case errors.Is(err, ErrInvalidStatus):
		return statusWithErrorInfo(codes.FailedPrecondition, ReasonInvalidStatus)
	case errors.Is(err, ErrNotificationsNotEnabled):
		return statusWithErrorInfo(codes.FailedPrecondition, ReasonNotificationsNotEnabled)
	case errors.Is(err, ErrNodeIsNotMember):
		return statusWithErrorInfo(codes.Aborted, ReasonNodeIsNotMember)
	case errors.Is(err, ErrNodeIsNotLeader):
		return statusWithErrorInfo(codes.Aborted, ReasonNodeIsNotLeader)
	case errors.Is(err, ErrNotInitialized):
		return statusWithErrorInfo(codes.Unavailable, ReasonNotInitialized)
	case errors.Is(err, ErrResourceUnavailable):
		return statusWithErrorInfo(codes.Unavailable, ReasonResourceUnavailable)
	default:
		return status.New(codes.Unknown, err.Error())
	}
}

func FromGrpcError(err error) (error, ErrorMetadata) {
	st := status.Convert(err)
	if st == nil {
		return err, nil
	}

	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.Domain != OxiaErrorDomain {
			continue
		}
		switch info.Reason {
		case ReasonAborted:
			return ErrAborted, info.Metadata
		case ReasonInvalidSessionTimeout:
			return ErrInvalidSessionTimeout, info.Metadata
		case ReasonSessionNotFound:
			return ErrSessionNotFound, info.Metadata
		case ReasonNamespaceNotFound:
			return ErrNamespaceNotFound, info.Metadata
		case ReasonInvalidTerm:
			return ErrInvalidTerm, info.Metadata
		case ReasonInvalidStatus:
			return ErrInvalidStatus, info.Metadata
		case ReasonNotificationsNotEnabled:
			return ErrNotificationsNotEnabled, info.Metadata
		case ReasonNodeIsNotMember:
			return ErrNodeIsNotMember, info.Metadata
		case ReasonNodeIsNotLeader:
			return ErrNodeIsNotLeader, info.Metadata
		case ReasonNotInitialized:
			return ErrNotInitialized, info.Metadata
		case ReasonResourceUnavailable:
			return ErrResourceUnavailable, info.Metadata
		default:
			return err, info.Metadata
		}
	}
	return err, nil
}

func IsRetryable(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, ErrAborted) ||
		errors.Is(err, ErrNodeIsNotMember) ||
		errors.Is(err, ErrNodeIsNotLeader) ||
		errors.Is(err, ErrNotInitialized) ||
		errors.Is(err, ErrResourceUnavailable)
}
