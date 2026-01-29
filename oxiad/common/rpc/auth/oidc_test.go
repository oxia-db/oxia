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

package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadPublicKeysFromFile(t *testing.T) {
	t.Run("Valid RSA Public Key", func(t *testing.T) {
		// Generate RSA key pair
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		// Create temporary file with public key
		tmpDir := t.TempDir()
		keyFile := filepath.Join(tmpDir, "public.pem")

		publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
		require.NoError(t, err)

		pemBlock := &pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: publicKeyBytes,
		}

		err = os.WriteFile(keyFile, pem.EncodeToMemory(pemBlock), 0600)
		require.NoError(t, err)

		// Test loading
		keys, err := loadPublicKeysFromFile(keyFile)
		assert.NoError(t, err)
		assert.Len(t, keys, 1)
	})

	t.Run("Multiple Keys", func(t *testing.T) {
		// Generate two RSA key pairs
		privateKey1, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		privateKey2, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		// Create temporary file with multiple public keys
		tmpDir := t.TempDir()
		keyFile := filepath.Join(tmpDir, "public.pem")

		publicKeyBytes1, err := x509.MarshalPKIXPublicKey(&privateKey1.PublicKey)
		require.NoError(t, err)
		publicKeyBytes2, err := x509.MarshalPKIXPublicKey(&privateKey2.PublicKey)
		require.NoError(t, err)

		pemBlock1 := &pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: publicKeyBytes1,
		}
		pemBlock2 := &pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: publicKeyBytes2,
		}

		var pemData []byte
		pemData = append(pemData, pem.EncodeToMemory(pemBlock1)...)
		pemData = append(pemData, pem.EncodeToMemory(pemBlock2)...)

		err = os.WriteFile(keyFile, pemData, 0600)
		require.NoError(t, err)

		// Test loading
		keys, err := loadPublicKeysFromFile(keyFile)
		assert.NoError(t, err)
		assert.Len(t, keys, 2)
	})

	t.Run("File Not Found", func(t *testing.T) {
		_, err := loadPublicKeysFromFile("/nonexistent/file.pem")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read key file")
	})

	t.Run("Empty File", func(t *testing.T) {
		tmpDir := t.TempDir()
		keyFile := filepath.Join(tmpDir, "empty.pem")
		err := os.WriteFile(keyFile, []byte(""), 0600)
		require.NoError(t, err)

		_, err = loadPublicKeysFromFile(keyFile)
		assert.Equal(t, ErrNoPublicKeysFound, err)
	})

	t.Run("Invalid PEM Data", func(t *testing.T) {
		tmpDir := t.TempDir()
		keyFile := filepath.Join(tmpDir, "invalid.pem")
		err := os.WriteFile(keyFile, []byte("not a pem file"), 0600)
		require.NoError(t, err)

		_, err = loadPublicKeysFromFile(keyFile)
		assert.Equal(t, ErrNoPublicKeysFound, err)
	})
}

func TestNewOIDCProviderWithStaticKeyFile(t *testing.T) {
	// Generate RSA key pair for testing
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create temporary file with public key
	tmpDir := t.TempDir()
	keyFile := filepath.Join(tmpDir, "public.pem")

	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)

	pemBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	}

	err = os.WriteFile(keyFile, pem.EncodeToMemory(pemBlock), 0600)
	require.NoError(t, err)

	t.Run("With Static Key File", func(t *testing.T) {
		options := OIDCOptions{
			AllowedIssueURLs: "https://example.com",
			AllowedAudiences: "test-audience",
			UserNameClaim:    "sub",
			StaticKeyFile:    keyFile,
		}
		jsonParams, err := json.Marshal(options)
		require.NoError(t, err)

		provider, err := NewOIDCProvider(context.Background(), string(jsonParams))
		assert.NoError(t, err)
		assert.NotNil(t, provider)

		oidcProvider, ok := provider.(*OIDCProvider)
		require.True(t, ok)
		assert.Equal(t, "sub", oidcProvider.userNameClaim)
		assert.Contains(t, oidcProvider.allowedAudiences, "test-audience")
		assert.Contains(t, oidcProvider.providers, "https://example.com")
	})

	t.Run("Without Static Key File (Backward Compatibility)", func(t *testing.T) {
		// This test will fail with remote JWKS, which is expected in a unit test
		// We're just testing that the code path is taken
		options := OIDCOptions{
			AllowedIssueURLs: "https://example.com",
			AllowedAudiences: "test-audience",
			UserNameClaim:    "sub",
			// No StaticKeyFile
		}
		jsonParams, err := json.Marshal(options)
		require.NoError(t, err)

		// This will fail because we can't reach example.com, but that's expected
		// The important thing is that it tries to use the remote provider
		_, err = NewOIDCProvider(context.Background(), string(jsonParams))
		assert.Error(t, err) // Expected to fail in unit test without real OIDC server
	})

	t.Run("With Invalid Key File", func(t *testing.T) {
		options := OIDCOptions{
			AllowedIssueURLs: "https://example.com",
			AllowedAudiences: "test-audience",
			StaticKeyFile:    "/nonexistent/file.pem",
		}
		jsonParams, err := json.Marshal(options)
		require.NoError(t, err)

		_, err = NewOIDCProvider(context.Background(), string(jsonParams))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read key file")
	})
}

func TestOIDCProviderAuthenticateWithStaticKeys(t *testing.T) {
	// Generate RSA key pair
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create temporary file with public key
	tmpDir := t.TempDir()
	keyFile := filepath.Join(tmpDir, "public.pem")

	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)

	pemBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	}

	err = os.WriteFile(keyFile, pem.EncodeToMemory(pemBlock), 0600)
	require.NoError(t, err)

	// Create OIDC provider with static key
	issuer := "https://test.example.com"
	audience := "test-audience"
	options := OIDCOptions{
		AllowedIssueURLs: issuer,
		AllowedAudiences: audience,
		UserNameClaim:    "sub",
		StaticKeyFile:    keyFile,
	}
	jsonParams, err := json.Marshal(options)
	require.NoError(t, err)

	provider, err := NewOIDCProvider(context.Background(), string(jsonParams))
	require.NoError(t, err)

	t.Run("Valid Token", func(t *testing.T) {
		// Create a valid JWT token
		claims := jwt.RegisteredClaims{
			Issuer:    issuer,
			Subject:   "test-user",
			Audience:  jwt.ClaimStrings{audience},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		}

		token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		signedToken, err := token.SignedString(privateKey)
		require.NoError(t, err)

		// Authenticate
		username, err := provider.Authenticate(context.Background(), signedToken)
		assert.NoError(t, err)
		assert.Equal(t, "test-user", username)
	})

	t.Run("Expired Token", func(t *testing.T) {
		// Create an expired JWT token
		claims := jwt.RegisteredClaims{
			Issuer:    issuer,
			Subject:   "test-user",
			Audience:  jwt.ClaimStrings{audience},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-1 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now().Add(-2 * time.Hour)),
		}

		token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		signedToken, err := token.SignedString(privateKey)
		require.NoError(t, err)

		// Authenticate should fail
		_, err = provider.Authenticate(context.Background(), signedToken)
		assert.Error(t, err)
	})

	t.Run("Wrong Audience", func(t *testing.T) {
		// Create a token with wrong audience
		claims := jwt.RegisteredClaims{
			Issuer:    issuer,
			Subject:   "test-user",
			Audience:  jwt.ClaimStrings{"wrong-audience"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		}

		token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		signedToken, err := token.SignedString(privateKey)
		require.NoError(t, err)

		// Authenticate should fail
		_, err = provider.Authenticate(context.Background(), signedToken)
		assert.Equal(t, ErrForbiddenAudience, err)
	})

	t.Run("Wrong Issuer", func(t *testing.T) {
		// Create a token with wrong issuer
		claims := jwt.RegisteredClaims{
			Issuer:    "https://wrong-issuer.com",
			Subject:   "test-user",
			Audience:  jwt.ClaimStrings{audience},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		}

		token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		signedToken, err := token.SignedString(privateKey)
		require.NoError(t, err)

		// Authenticate should fail
		_, err = provider.Authenticate(context.Background(), signedToken)
		assert.Equal(t, ErrUnknownIssuer, err)
	})

	t.Run("Malformed Token", func(t *testing.T) {
		_, err := provider.Authenticate(context.Background(), "not.a.valid.token")
		assert.Equal(t, ErrMalformedToken, err)
	})
}

func TestOIDCOptionsValidate(t *testing.T) {
	t.Run("Valid Options", func(t *testing.T) {
		options := &OIDCOptions{
			AllowedIssueURLs: "https://example.com",
			AllowedAudiences: "test-audience",
		}
		err := options.Validate()
		assert.NoError(t, err)
	})

	t.Run("Empty Issuer URL", func(t *testing.T) {
		options := &OIDCOptions{
			AllowedAudiences: "test-audience",
		}
		err := options.Validate()
		assert.Equal(t, ErrEmptyIssueURL, err)
	})

	t.Run("Empty Audiences", func(t *testing.T) {
		options := &OIDCOptions{
			AllowedIssueURLs: "https://example.com",
		}
		err := options.Validate()
		assert.Equal(t, ErrEmptyAllowedAudiences, err)
	})
}

func TestOIDCOptionsWithDefault(t *testing.T) {
	t.Run("Sets Default UserNameClaim", func(t *testing.T) {
		options := &OIDCOptions{
			AllowedIssueURLs: "https://example.com",
			AllowedAudiences: "test-audience",
		}
		options.withDefault()
		assert.Equal(t, DefaultUserNameCalm, options.UserNameClaim)
	})

	t.Run("Preserves Custom UserNameClaim", func(t *testing.T) {
		options := &OIDCOptions{
			AllowedIssueURLs: "https://example.com",
			AllowedAudiences: "test-audience",
			UserNameClaim:    "email",
		}
		options.withDefault()
		assert.Equal(t, "email", options.UserNameClaim)
	})
}
