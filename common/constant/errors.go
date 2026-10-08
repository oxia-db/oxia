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

package constant

import (
	"errors"
	"io"
	"strconv"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const OxiaErrorDomain = "oxia.io"

const (
	ErrorMetadataShard             = "shard"
	ErrorMetadataLeader            = "leader"
	ErrorMetadataCoordinatorLeader = "coordinator-leader"
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

func (m ErrorMetadata) GetCoordinatorLeaderHint() (string, bool) {
	leader := m[ErrorMetadataCoordinatorLeader]
	return leader, leader != ""
}

const (
	ReasonAborted                 string = "ABORTED"
	ReasonInvalidSessionTimeout   string = "INVALID_SESSION_TIMEOUT"
	ReasonSessionNotFound         string = "SESSION_NOT_FOUND"
	ReasonNamespaceNotFound       string = "NAMESPACE_NOT_FOUND"
	ReasonShardNotFound           string = "SHARD_NOT_FOUND"
	ReasonInvalidTerm             string = "INVALID_TERM"
	ReasonInvalidStatus           string = "INVALID_STATUS"
	ReasonNotificationsNotEnabled string = "NOTIFICATIONS_NOT_ENABLED"
	ReasonNodeIsNotMember         string = "NODE_IS_NOT_MEMBER"
	ReasonNodeIsNotLeader         string = "NODE_IS_NOT_LEADER"
	ReasonNotInitialized          string = "NOT_INITIALIZED"
	ReasonResourceConflict        string = "RESOURCE_CONFLICT"
	ReasonResourceUnavailable     string = "RESOURCE_UNAVAILABLE"
	ReasonUnsupportedFeatures     string = "UNSUPPORTED_FEATURES"
	ReasonUnknown                        = "UNKNOWN"
)

var (
	ErrAborted                 = errors.New("oxia: operation was aborted")
	ErrInvalidSessionTimeout   = errors.New("oxia: invalid session timeout")
	ErrSessionNotFound         = errors.New("oxia: session not found")
	ErrNamespaceNotFound       = errors.New("oxia: namespace not found")
	ErrShardNotFound           = errors.New("oxia: shard not found")
	ErrInvalidTerm             = errors.New("oxia: invalid term")
	ErrInvalidStatus           = errors.New("oxia: invalid status")
	ErrNotificationsNotEnabled = errors.New("oxia: notifications not enabled on namespace")
	ErrNodeIsNotMember         = errors.New("oxia: node is not a member")
	ErrNodeIsNotLeader         = errors.New("oxia: node is not leader")
	ErrNotInitialized          = errors.New("oxia: server not initialized yet")
	ErrResourceConflict        = errors.New("oxia: resource conflict")
	ErrResourceUnavailable     = errors.New("oxia: resource unavailable")
	ErrUnsupportedFeatures     = errors.New("oxia: unsupported features")
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

func WithCoordinatorLeaderHint(leader string) GrpcStatusOption {
	return func(metadata ErrorMetadata) {
		if leader == "" {
			return
		}
		metadata[ErrorMetadataCoordinatorLeader] = leader
	}
}

func IntoGrpcStatusError(err error, opts ...GrpcStatusOption) error { //nolint:revive // Keep the explicit error-to-status mapping readable.
	if err == nil {
		return nil
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

	if st, ok := status.FromError(err); ok {
		return withErrorInfoDetails(st, ReasonUnknown, metadata)
	}

	switch {
	case errors.Is(err, ErrAborted):
		return withErrorInfoDetails(status.New(codes.Aborted, err.Error()), ReasonAborted, metadata)
	case errors.Is(err, ErrInvalidSessionTimeout):
		return withErrorInfoDetails(status.New(codes.InvalidArgument, err.Error()), ReasonInvalidSessionTimeout, metadata)
	case errors.Is(err, ErrSessionNotFound):
		return withErrorInfoDetails(status.New(codes.NotFound, err.Error()), ReasonSessionNotFound, metadata)
	case errors.Is(err, ErrNamespaceNotFound):
		return withErrorInfoDetails(status.New(codes.NotFound, err.Error()), ReasonNamespaceNotFound, metadata)
	case errors.Is(err, ErrShardNotFound):
		return withErrorInfoDetails(status.New(codes.NotFound, err.Error()), ReasonShardNotFound, metadata)
	case errors.Is(err, ErrInvalidTerm):
		return withErrorInfoDetails(status.New(codes.FailedPrecondition, err.Error()), ReasonInvalidTerm, metadata)
	case errors.Is(err, ErrInvalidStatus):
		return withErrorInfoDetails(status.New(codes.FailedPrecondition, err.Error()), ReasonInvalidStatus, metadata)
	case errors.Is(err, ErrNotificationsNotEnabled):
		return withErrorInfoDetails(status.New(codes.FailedPrecondition, err.Error()), ReasonNotificationsNotEnabled, metadata)
	case errors.Is(err, ErrNodeIsNotMember):
		return withErrorInfoDetails(status.New(codes.Aborted, err.Error()), ReasonNodeIsNotMember, metadata)
	case errors.Is(err, ErrNodeIsNotLeader):
		return withErrorInfoDetails(status.New(codes.Aborted, err.Error()), ReasonNodeIsNotLeader, metadata)
	case errors.Is(err, ErrNotInitialized):
		return withErrorInfoDetails(status.New(codes.Unavailable, err.Error()), ReasonNotInitialized, metadata)
	case errors.Is(err, ErrResourceConflict):
		return withErrorInfoDetails(status.New(codes.FailedPrecondition, err.Error()), ReasonResourceConflict, metadata)
	case errors.Is(err, ErrResourceUnavailable):
		return withErrorInfoDetails(status.New(codes.Unavailable, err.Error()), ReasonResourceUnavailable, metadata)
	case errors.Is(err, ErrUnsupportedFeatures):
		return withErrorInfoDetails(status.New(codes.FailedPrecondition, err.Error()), ReasonUnsupportedFeatures, metadata)
	default:
		return status.Error(codes.Unknown, err.Error())
	}
}

func withErrorInfoDetails(st *status.Status, reason string, metadata ErrorMetadata) error {
	stWithDetails, detailsErr := st.WithDetails(&errdetails.ErrorInfo{
		Reason:   reason,
		Domain:   OxiaErrorDomain,
		Metadata: metadata,
	})
	if detailsErr != nil {
		return st.Err()
	}
	return stWithDetails.Err()
}

func FromGrpcError(err error) (error, ErrorMetadata) { //nolint:revive,staticcheck // Keep translated error first for provider call sites.
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
		case ReasonShardNotFound:
			return ErrShardNotFound, info.Metadata
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
		case ReasonResourceConflict:
			return ErrResourceConflict, info.Metadata
		case ReasonResourceUnavailable:
			return ErrResourceUnavailable, info.Metadata
		case ReasonUnsupportedFeatures:
			return ErrUnsupportedFeatures, info.Metadata
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
