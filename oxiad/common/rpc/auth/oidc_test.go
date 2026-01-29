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
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOIDCOptions_Validate tests validation of OIDC options
func TestOIDCOptions_Validate(t *testing.T) {
	tests := []struct {
		name    string
		options OIDCOptions
		wantErr bool
	}{
		{
			name: "valid legacy configuration",
			options: OIDCOptions{
				AllowedIssueURLs: "https://issuer1.com",
				AllowedAudiences: "audience1",
			},
			wantErr: false,
		},
		{
			name: "valid new configuration",
			options: OIDCOptions{
				Issuers: map[string]IssuerConfig{
					"https://issuer1.com": {
						AllowedAudiences: "audience1",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid new configuration with static key file",
			options: OIDCOptions{
				Issuers: map[string]IssuerConfig{
					"https://issuer1.com": {
						AllowedAudiences: "audience1",
						StaticKeyFile:    "/path/to/key.pem",
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "empty configuration",
			options: OIDCOptions{},
			wantErr: true,
		},
		{
			name: "legacy configuration missing audiences",
			options: OIDCOptions{
				AllowedIssueURLs: "https://issuer1.com",
			},
			wantErr: true,
		},
		{
			name: "new configuration missing audiences",
			options: OIDCOptions{
				Issuers: map[string]IssuerConfig{
					"https://issuer1.com": {},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.options.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestOIDCOptions_WithDefault tests default value application
func TestOIDCOptions_WithDefault(t *testing.T) {
	t.Run("apply default to legacy config", func(t *testing.T) {
		opts := &OIDCOptions{
			AllowedIssueURLs: "https://issuer1.com",
			AllowedAudiences: "audience1",
		}
		opts.withDefault()
		assert.Equal(t, DefaultUserNameCalm, opts.UserNameClaim)
	})

	t.Run("apply default to new config", func(t *testing.T) {
		opts := &OIDCOptions{
			Issuers: map[string]IssuerConfig{
				"https://issuer1.com": {
					AllowedAudiences: "audience1",
				},
			},
		}
		opts.withDefault()
		assert.Equal(t, DefaultUserNameCalm, opts.Issuers["https://issuer1.com"].UserNameClaim)
	})

	t.Run("preserve existing userNameClaim in new config", func(t *testing.T) {
		opts := &OIDCOptions{
			Issuers: map[string]IssuerConfig{
				"https://issuer1.com": {
					AllowedAudiences: "audience1",
					UserNameClaim:    "email",
				},
			},
		}
		opts.withDefault()
		assert.Equal(t, "email", opts.Issuers["https://issuer1.com"].UserNameClaim)
	})
}

// TestNewOIDCProvider_LegacyConfiguration tests backward compatibility with legacy config
func TestNewOIDCProvider_LegacyConfiguration(t *testing.T) {
	mockOIDC, err := mockoidc.Run()
	require.NoError(t, err)
	defer func() {
		_ = mockOIDC.Shutdown()
	}()

	audience := "test-audience"
	options := OIDCOptions{
		AllowedIssueURLs: mockOIDC.Issuer(),
		AllowedAudiences: audience,
		UserNameClaim:    "sub",
	}
	jsonParams, err := json.Marshal(options)
	require.NoError(t, err)

	provider, err := NewOIDCProvider(context.Background(), string(jsonParams))
	require.NoError(t, err)
	require.NotNil(t, provider)

	oidcProvider, ok := provider.(*OIDCProvider)
	require.True(t, ok)
	assert.Equal(t, "sub", oidcProvider.userNameClaim)
	assert.Contains(t, oidcProvider.allowedAudiences, audience)
	assert.Len(t, oidcProvider.providers, 1)
}

// TestNewOIDCProvider_NewConfiguration tests new per-issuer configuration
func TestNewOIDCProvider_NewConfiguration(t *testing.T) {
	mockOIDC1, err := mockoidc.Run()
	require.NoError(t, err)
	defer func() {
		_ = mockOIDC1.Shutdown()
	}()

	mockOIDC2, err := mockoidc.Run()
	require.NoError(t, err)
	defer func() {
		_ = mockOIDC2.Shutdown()
	}()

	options := OIDCOptions{
		Issuers: map[string]IssuerConfig{
			mockOIDC1.Issuer(): {
				AllowedAudiences: "audience1",
				UserNameClaim:    "sub",
			},
			mockOIDC2.Issuer(): {
				AllowedAudiences: "audience2",
				UserNameClaim:    "email",
			},
		},
	}
	jsonParams, err := json.Marshal(options)
	require.NoError(t, err)

	provider, err := NewOIDCProvider(context.Background(), string(jsonParams))
	require.NoError(t, err)
	require.NotNil(t, provider)

	oidcProvider, ok := provider.(*OIDCProvider)
	require.True(t, ok)
	assert.Len(t, oidcProvider.providers, 2)

	// Verify issuer 1 configuration
	providerWithVerifier1 := oidcProvider.providers[mockOIDC1.Issuer()]
	require.NotNil(t, providerWithVerifier1)
	assert.Equal(t, "sub", providerWithVerifier1.userNameClaim)
	assert.Contains(t, providerWithVerifier1.allowedAudiences, "audience1")

	// Verify issuer 2 configuration
	providerWithVerifier2 := oidcProvider.providers[mockOIDC2.Issuer()]
	require.NotNil(t, providerWithVerifier2)
	assert.Equal(t, "email", providerWithVerifier2.userNameClaim)
	assert.Contains(t, providerWithVerifier2.allowedAudiences, "audience2")
}

// TestNewOIDCProvider_WithStaticKeyFile tests static key file support
func TestNewOIDCProvider_WithStaticKeyFile(t *testing.T) {
	// Generate a test RSA key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create a temporary key file
	tempFile, err := os.CreateTemp("", "test-key-*.pem")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	// Write public key to file
	pubKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)
	err = pem.Encode(tempFile, &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubKeyBytes,
	})
	require.NoError(t, err)
	tempFile.Close()

	issuerURL := "https://custom-issuer.com"
	options := OIDCOptions{
		Issuers: map[string]IssuerConfig{
			issuerURL: {
				AllowedAudiences: "test-audience",
				UserNameClaim:    "sub",
				StaticKeyFile:    tempFile.Name(),
			},
		},
	}
	jsonParams, err := json.Marshal(options)
	require.NoError(t, err)

	provider, err := NewOIDCProvider(context.Background(), string(jsonParams))
	require.NoError(t, err)
	require.NotNil(t, provider)

	oidcProvider, ok := provider.(*OIDCProvider)
	require.True(t, ok)
	assert.Len(t, oidcProvider.providers, 1)

	providerWithVerifier := oidcProvider.providers[issuerURL]
	require.NotNil(t, providerWithVerifier)
	assert.Nil(t, providerWithVerifier.provider) // No remote provider when using static keys
	assert.NotNil(t, providerWithVerifier.verifier)
}

// TestOIDCProvider_Authenticate_Legacy tests authentication with legacy configuration
func TestOIDCProvider_Authenticate_Legacy(t *testing.T) {
	mockOIDC, err := mockoidc.Run()
	require.NoError(t, err)
	defer func() {
		_ = mockOIDC.Shutdown()
	}()

	audience := "test-audience"
	subject := "test-user"

	options := OIDCOptions{
		AllowedIssueURLs: mockOIDC.Issuer(),
		AllowedAudiences: audience,
		UserNameClaim:    "sub",
	}
	jsonParams, err := json.Marshal(options)
	require.NoError(t, err)

	provider, err := NewOIDCProvider(context.Background(), string(jsonParams))
	require.NoError(t, err)

	// Create a valid token
	claims := &jwt.RegisteredClaims{
		Audience:  jwt.ClaimStrings{audience},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
		Issuer:    mockOIDC.Issuer(),
		Subject:   subject,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
	}
	token, err := mockOIDC.Keypair.SignJWT(claims)
	require.NoError(t, err)

	// Authenticate
	userName, err := provider.Authenticate(context.Background(), token)
	require.NoError(t, err)
	assert.Equal(t, subject, userName)
}

// TestOIDCProvider_Authenticate_PerIssuer tests authentication with per-issuer configuration
func TestOIDCProvider_Authenticate_PerIssuer(t *testing.T) {
	mockOIDC1, err := mockoidc.Run()
	require.NoError(t, err)
	defer func() {
		_ = mockOIDC1.Shutdown()
	}()

	mockOIDC2, err := mockoidc.Run()
	require.NoError(t, err)
	defer func() {
		_ = mockOIDC2.Shutdown()
	}()

	options := OIDCOptions{
		Issuers: map[string]IssuerConfig{
			mockOIDC1.Issuer(): {
				AllowedAudiences: "audience1",
				UserNameClaim:    "sub",
			},
			mockOIDC2.Issuer(): {
				AllowedAudiences: "audience2",
				UserNameClaim:    "sub",
			},
		},
	}
	jsonParams, err := json.Marshal(options)
	require.NoError(t, err)

	provider, err := NewOIDCProvider(context.Background(), string(jsonParams))
	require.NoError(t, err)

	t.Run("authenticate with issuer 1", func(t *testing.T) {
		claims := &jwt.RegisteredClaims{
			Audience:  jwt.ClaimStrings{"audience1"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
			Issuer:    mockOIDC1.Issuer(),
			Subject:   "user1",
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		}
		token, err := mockOIDC1.Keypair.SignJWT(claims)
		require.NoError(t, err)

		userName, err := provider.Authenticate(context.Background(), token)
		require.NoError(t, err)
		assert.Equal(t, "user1", userName)
	})

	t.Run("authenticate with issuer 2", func(t *testing.T) {
		claims := &jwt.RegisteredClaims{
			Audience:  jwt.ClaimStrings{"audience2"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
			Issuer:    mockOIDC2.Issuer(),
			Subject:   "user2",
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		}
		token, err := mockOIDC2.Keypair.SignJWT(claims)
		require.NoError(t, err)

		userName, err := provider.Authenticate(context.Background(), token)
		require.NoError(t, err)
		assert.Equal(t, "user2", userName)
	})

	t.Run("reject token with wrong audience", func(t *testing.T) {
		claims := &jwt.RegisteredClaims{
			Audience:  jwt.ClaimStrings{"wrong-audience"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
			Issuer:    mockOIDC1.Issuer(),
			Subject:   "user1",
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		}
		token, err := mockOIDC1.Keypair.SignJWT(claims)
		require.NoError(t, err)

		_, err = provider.Authenticate(context.Background(), token)
		assert.ErrorIs(t, err, ErrForbiddenAudience)
	})
}

// TestOIDCProvider_Authenticate_Errors tests authentication error cases
func TestOIDCProvider_Authenticate_Errors(t *testing.T) {
	mockOIDC, err := mockoidc.Run()
	require.NoError(t, err)
	defer func() {
		_ = mockOIDC.Shutdown()
	}()

	options := OIDCOptions{
		AllowedIssueURLs: mockOIDC.Issuer(),
		AllowedAudiences: "test-audience",
	}
	jsonParams, err := json.Marshal(options)
	require.NoError(t, err)

	provider, err := NewOIDCProvider(context.Background(), string(jsonParams))
	require.NoError(t, err)

	t.Run("invalid token type", func(t *testing.T) {
		_, err := provider.Authenticate(context.Background(), 123)
		assert.ErrorIs(t, err, ErrUnMatchedAuthenticationParamType)
	})

	t.Run("malformed token", func(t *testing.T) {
		_, err := provider.Authenticate(context.Background(), "invalid-token")
		assert.ErrorIs(t, err, ErrMalformedToken)
	})

	t.Run("unknown issuer", func(t *testing.T) {
		// Create token with different issuer
		unknownIssuer := "https://unknown-issuer.com"
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss": unknownIssuer,
			"sub": "user",
			"aud": "test-audience",
			"exp": time.Now().Add(1 * time.Hour).Unix(),
		})
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		signedToken, err := token.SignedString(privateKey)
		require.NoError(t, err)

		_, err = provider.Authenticate(context.Background(), signedToken)
		assert.ErrorIs(t, err, ErrUnknownIssuer)
	})
}

// TestLoadPublicKeysFromFile tests loading public keys from PEM files
func TestLoadPublicKeysFromFile(t *testing.T) {
	t.Run("load RSA public key", func(t *testing.T) {
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		tempFile, err := os.CreateTemp("", "test-rsa-*.pem")
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		pubKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
		require.NoError(t, err)
		err = pem.Encode(tempFile, &pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: pubKeyBytes,
		})
		require.NoError(t, err)
		tempFile.Close()

		keys, err := loadPublicKeysFromFile(tempFile.Name())
		require.NoError(t, err)
		assert.Len(t, keys, 1)
	})

	t.Run("load PKCS1 RSA public key", func(t *testing.T) {
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		tempFile, err := os.CreateTemp("", "test-rsa-pkcs1-*.pem")
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		pubKeyBytes := x509.MarshalPKCS1PublicKey(&privateKey.PublicKey)
		err = pem.Encode(tempFile, &pem.Block{
			Type:  "RSA PUBLIC KEY",
			Bytes: pubKeyBytes,
		})
		require.NoError(t, err)
		tempFile.Close()

		keys, err := loadPublicKeysFromFile(tempFile.Name())
		require.NoError(t, err)
		assert.Len(t, keys, 1)
	})

	t.Run("file not found", func(t *testing.T) {
		_, err := loadPublicKeysFromFile("/nonexistent/file.pem")
		assert.Error(t, err)
	})

	t.Run("empty file", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test-empty-*.pem")
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())
		tempFile.Close()

		_, err = loadPublicKeysFromFile(tempFile.Name())
		assert.ErrorIs(t, err, ErrNoPublicKeysFound)
	})
}
