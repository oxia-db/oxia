// Copyright 2024 StreamNative, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/json"

	"github.com/oxia-db/oxia/coordinator"
	"github.com/oxia-db/oxia/coordinator/metadata"
	rpc2 "github.com/oxia-db/oxia/coordinator/rpc"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/oxia"
	clientauth "github.com/oxia-db/oxia/oxia/auth"
	"github.com/oxia-db/oxia/server"
	"github.com/oxia-db/oxia/server/auth"
)

func newOxiaClusterWithAuth(t *testing.T, issueURL string, audiences string) (address string, closeFunc func()) {
	t.Helper()
	options := auth.OIDCOptions{
		AllowedIssueURLs: issueURL,
		AllowedAudiences: audiences,
	}
	jsonParams, err := json.Marshal(options)
	assert.NoError(t, err)
	authParams := auth.Options{
		ProviderName:   auth.ProviderOIDC,
		ProviderParams: string(jsonParams),
	}
	s1, err := server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
		AuthOptions:                authParams,
	})
	assert.NoError(t, err)
	s1Addr := model.Server{
		Public:   fmt.Sprintf("localhost:%d", s1.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s1.InternalPort()),
	}
	s2, err := server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
		AuthOptions:                authParams,
	})
	assert.NoError(t, err)
	s2Addr := model.Server{
		Public:   fmt.Sprintf("localhost:%d", s2.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s2.InternalPort()),
	}
	s3, err := server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
		AuthOptions:                authParams,
	})
	assert.NoError(t, err)
	s3Addr := model.Server{
		Public:   fmt.Sprintf("localhost:%d", s3.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s3.InternalPort()),
	}

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{s1Addr, s2Addr, s3Addr},
	}

	clientPool := rpc.NewClientPool(nil, nil)

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider,
		func() (model.ClusterConfig, error) { return clusterConfig, nil },
		nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)

	return s1Addr.Public, func() {
		s1.Close()
		s2.Close()
		s3.Close()

		clientPool.Close()
		coordinatorInstance.Close()
	}
}

func TestOIDCWithStaticToken(t *testing.T) {
	mockOIDC, err := mockoidc.Run()
	assert.NoError(t, err)
	defer func(mockOIDC *mockoidc.MockOIDC) {
		_ = mockOIDC.Shutdown()
	}(mockOIDC)

	audience := generateRandomStr(t)
	audience2 := generateRandomStr(t)
	id := generateRandomStr(t)
	subject := generateRandomStr(t)
	registeredClaims := &jwt.RegisteredClaims{
		Audience:  jwt.ClaimStrings{audience, audience2},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Duration(1) * time.Hour)),
		ID:        id,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		Issuer:    mockOIDC.Issuer(),
		NotBefore: jwt.NewNumericDate(time.Time{}),
		Subject:   subject,
	}
	signedToken, err := mockOIDC.Keypair.SignJWT(registeredClaims)
	assert.NoError(t, err)

	addr, clusterCloseFunc := newOxiaClusterWithAuth(t, mockOIDC.Issuer(), audience)
	defer clusterCloseFunc()

	// assert connection failed with empty token
	_, err = oxia.NewSyncClient(addr)
	assert.Equal(t, codes.Unauthenticated, status.Code(errors.Unwrap(err)))

	// assert connection failed with malformed token
	_, err = oxia.NewSyncClient(addr,
		oxia.WithAuthentication(clientauth.NewTokenAuthenticationWithToken("wrongToken", false)))
	assert.Equal(t, codes.Unauthenticated, status.Code(errors.Unwrap(err)))

	// assert connection failed with unknown issue
	_, err = oxia.NewSyncClient(addr,
		oxia.WithAuthentication(clientauth.NewTokenAuthenticationWithToken(illegalToken, false)))
	assert.Equal(t, codes.Unauthenticated, status.Code(errors.Unwrap(err)))

	cutToken := signedToken[5:]
	_, err = oxia.NewSyncClient(addr,
		oxia.WithAuthentication(clientauth.NewTokenAuthenticationWithToken(cutToken, false)))
	assert.Equal(t, codes.Unauthenticated, status.Code(errors.Unwrap(err)))

	// assert connection failed with expired token
	expiredToken, err := mockOIDC.Keypair.SignJWT(&jwt.RegisteredClaims{
		Audience:  jwt.ClaimStrings{audience},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Duration(1) * time.Second)),
		ID:        id,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		Issuer:    mockOIDC.Issuer(),
		NotBefore: jwt.NewNumericDate(time.Time{}),
		Subject:   subject,
	})
	assert.NoError(t, err)
	time.Sleep(3 * time.Second)
	_, err = oxia.NewSyncClient(addr,
		oxia.WithAuthentication(clientauth.NewTokenAuthenticationWithToken(expiredToken, false)))
	assert.Equal(t, codes.Unauthenticated, status.Code(errors.Unwrap(err)))

	// assert connection success with correct token
	client, err := oxia.NewSyncClient(addr,
		oxia.WithAuthentication(clientauth.NewTokenAuthenticationWithToken(signedToken, false)))
	assert.NoError(t, err)
	ctx := context.Background()
	key := "hi"
	payload := []byte("matt")
	_, pVersion, err := client.Put(ctx, key, payload)
	assert.NoError(t, err)
	_, gValue, gVersion, err := client.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, pVersion, gVersion)
	assert.Equal(t, gValue, payload)
	client.Close()
}

func generateRandomStr(t *testing.T) string {
	t.Helper()
	random, err := uuid.NewRandom()
	assert.NoError(t, err)
	return random.String()
}

const (
	illegalToken = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImRIWFRTQ3lvdXE2RGlXYVF3bFh0TlA1NC1DNzVtdzNJY29Za0VSZmwzZlEiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjYxMzM4L29pZGMiLCJzdWIiOiJjZGJiYWE4NC0xODg0LTQ5NTktOTgwNS02NGRiY2NiMzdlZTIiLCJhdWQiOlsiNDc5YzRmYTktNDZjMC00NjY5LTkyZTktY2QxYmM3Mjc2MWNlIl0sImV4cCI6MTcxOTI0MjM4OSwibmJmIjotNjIxMzU1OTY4MDAsImlhdCI6MTcxOTIzODc4OSwianRpIjoiODY1MTlkOGEtMmNiYy00NTY0LTlmZjMtNTUyZTAxYjQzNzc2In0.netDk-UFqBwlxJZlDc3Any2tSqBHXsLxdorM3MrL171Xql6Mms6KCiNabpWbx--xvPtvlzs3v1O1R5LO3bZbI1VO-efumOpvjDBxe6WRqeezGp1spcJ_s0M90MjF7d6uRDxlOfEmPaB1Oryb8kYlyErrdXM3P1jRN_i2HMdju0tKjEVcqIbuzBs5et3RrLHmcP5yMFB9D9xN4zeTd_Rf7Qyl1JdiA2qD-1KDfeVtGAahyuNiR0-VOncY1VU3sqi-h8cviyB7cn2j4Iuo5D-DIuvrbC-jS51NUSLb_nSD8LjuGoc76n3-_zB2svTFVv-1tiLESASqna4HaI_AyRSDNQ"
)
