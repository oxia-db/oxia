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

package server

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	MetadataAuthorizationKey = "authorization"
	TokenPrefix              = "Bearer "
)

var (
	ErrMetadataFetchFailed = errors.New("metadata fetch failed")
)

type GrpcAuthenticationDelegator struct {
	provider AuthenticationProvider

	validate func(ctx context.Context, provider AuthenticationProvider) (string, error)
}

func (delegator *GrpcAuthenticationDelegator) GetUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		_, err := delegator.validate(ctx, delegator.provider)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}
		// todo: set username to metadata to support authorization
		return handler(ctx, req)
	}
}

func (delegator *GrpcAuthenticationDelegator) GetStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		_, err := delegator.validate(ss.Context(), delegator.provider)
		if err != nil {
			return status.Error(codes.Unauthenticated, err.Error())
		}
		// todo: set username to metadata to support authorization
		return handler(srv, ss)
	}
}

func NewGrpcAuthenticationDelegator(provider AuthenticationProvider) (*GrpcAuthenticationDelegator, error) {
	delegator := &GrpcAuthenticationDelegator{
		provider: provider,
	}
	switch provider.AcceptParamType() {
	case ProviderParamTypeToken:
		delegator.validate = validateTokenWithContext
	default:
		return nil, ErrUnMatchedAuthenticationParamType
	}
	return delegator, nil
}

func validateTokenWithContext(ctx context.Context, provider AuthenticationProvider) (string, error) {
	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrMetadataFetchFailed
	}
	peerMeta, ok := peer.FromContext(ctx)
	if !ok {
		return "", ErrMetadataFetchFailed
	}
	val := meta.Get(MetadataAuthorizationKey)
	if len(val) < 1 {
		slog.Debug("Receive empty token from the client",
			slog.String("peer", peerMeta.Addr.String()))
		return "", ErrEmptyToken
	}
	token := strings.TrimPrefix(val[0], TokenPrefix)
	var userName string
	var err error
	if userName, err = provider.Authenticate(ctx, token); err != nil {
		slog.Debug("Failed to authenticate token",
			slog.String("peer", peerMeta.Addr.String()),
			slog.String("token", token))
		return "", err
	}
	return userName, nil
}
