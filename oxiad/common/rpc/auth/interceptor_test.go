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

package auth

import (
	"context"
	"net"
	"testing"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestGrpcAuthenticationDelegatorGetUnaryInterceptorCallsHandlerWithValidToken(t *testing.T) {
	provider := &recordingProvider{}
	delegator := NewGrpcAuthenticationDelegator(provider)
	ctx := metadata.NewIncomingContext(newPeerContext(), metadata.Pairs(MetadataAuthorizationKey, TokenPrefix+"token"))

	res, err := delegator.GetUnaryInterceptor()(ctx, "request", &grpc.UnaryServerInfo{},
		func(_ context.Context, req any) (any, error) {
			return req.(string) + "-handled", nil
		})

	require.NoError(t, err)
	assert.Equal(t, "request-handled", res)
	assert.Equal(t, "token", provider.token)
}

func TestGrpcAuthenticationDelegatorGetUnaryInterceptorRejectsMissingToken(t *testing.T) {
	delegator := NewGrpcAuthenticationDelegator(&recordingProvider{})
	handlerCalled := false

	res, err := delegator.GetUnaryInterceptor()(newPeerContext(), "request", &grpc.UnaryServerInfo{},
		func(context.Context, any) (any, error) {
			handlerCalled = true
			return "response", nil
		})

	assert.Nil(t, res)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), errs.ErrEmptyToken.Error())
	assert.False(t, handlerCalled)
}

func TestValidateTokenWithContext(t *testing.T) {
	peerCtx := peer.NewContext(context.Background(),
		&peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}})

	t.Run("token", func(t *testing.T) {
		provider := &recordingProvider{}
		ctx := metadata.NewIncomingContext(peerCtx, metadata.Pairs(
			":authority", "localhost:6648",
			MetadataAuthorizationKey, TokenPrefix+"token"))
		userName, err := validateTokenWithContext(ctx, provider)
		require.NoError(t, err)
		assert.Equal(t, "user", userName)
		assert.Equal(t, "token", provider.token)
	})

	t.Run("missing header", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(peerCtx, metadata.Pairs(":authority", "localhost:6648"))
		_, err := validateTokenWithContext(ctx, &recordingProvider{})
		assert.ErrorIs(t, err, errs.ErrEmptyToken)
	})

	t.Run("no metadata", func(t *testing.T) {
		_, err := validateTokenWithContext(peerCtx, &recordingProvider{})
		assert.ErrorIs(t, err, errs.ErrEmptyToken)
	})
}

func TestValidateTokenWithContextRejectsMissingPeer(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(MetadataAuthorizationKey, TokenPrefix+"token"))
	provider := &recordingProvider{}

	_, err := validateTokenWithContext(ctx, provider)

	assert.ErrorIs(t, err, errs.ErrMetadataFetchFailed)
	assert.Empty(t, provider.token)
}

func TestRedactToken(t *testing.T) {
	tests := []struct {
		name     string
		token    string
		expected string
	}{
		{"empty", "", "[REDACTED]"},
		{"short", "abc", "[REDACTED]"},
		{"exactly8", "12345678", "[REDACTED]"},
		{"9chars", "123456789", "[REDACTED]23456789"},
		{"starts with redaction prefix", "[REDACTED]123456789", "[REDACTED]23456789"},
		{"long token", "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.payload.signature", "[REDACTED]ignature"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := redactToken(tt.token)
			assert.Equal(t, tt.expected, result)
			// Ensure the redacted output never equals the original token
			assert.NotEqual(t, tt.token, result, "redacted output must differ from original token")
		})
	}
}

// recordingProvider accepts any token and records the last one.
type recordingProvider struct {
	token string
}

func (p *recordingProvider) Authenticate(_ context.Context, param any) (string, error) {
	p.token = param.(string)
	return "user", nil
}

func newPeerContext() context.Context {
	return peer.NewContext(context.Background(),
		&peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}})
}
