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
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/golang-jwt/jwt/v5"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOIDCOptions_Validate tests validation of OIDC options.
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

func TestOIDCOptionsValidateRejectsEmptyIssuerKey(t *testing.T) {
	options := OIDCOptions{Issuers: map[string]IssuerConfig{"": {AllowedAudiences: "audience"}}}

	assert.ErrorIs(t, options.Validate(), errs.ErrEmptyIssueURL)
}

// TestOIDCOptions_WithDefault tests default value application.
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

// TestNewOIDCProvider_LegacyConfiguration tests backward compatibility with legacy config.
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

// TestNewOIDCProvider_NewConfiguration tests new per-issuer configuration.
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

// TestNewOIDCProvider_WithStaticKeyFile tests static key file support.
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

func TestNewOIDCProviderRejectsInvalidJSON(t *testing.T) {
	provider, err := NewOIDCProvider(context.Background(), "{not json")

	assert.Error(t, err)
	assert.Nil(t, provider)
}

func TestNewOIDCProviderRejectsInvalidOptions(t *testing.T) {
	provider, err := NewOIDCProvider(context.Background(), "{}")

	assert.ErrorIs(t, err, errs.ErrEmptyIssueURL)
	assert.Nil(t, provider)
}

func TestNewOIDCProviderRejectsMissingStaticKeyFile(t *testing.T) {
	params := marshalOIDCOptions(t, OIDCOptions{Issuers: map[string]IssuerConfig{
		"https://issuer.example.com": {
			AllowedAudiences: "audience",
			StaticKeyFile:    filepath.Join(t.TempDir(), "missing.pem"),
		},
	}})

	provider, err := NewOIDCProvider(context.Background(), params)

	assert.ErrorContains(t, err, "failed to load static key file for issuer https://issuer.example.com")
	assert.Nil(t, provider)
}

func TestNewOIDCProviderRejectsUnreachableIssuer(t *testing.T) {
	issuer := newNotFoundIssuer(t)
	tests := []struct {
		name    string
		options OIDCOptions
	}{
		{"legacy", OIDCOptions{AllowedIssueURLs: issuer, AllowedAudiences: "audience"}},
		{"per issuer", OIDCOptions{Issuers: map[string]IssuerConfig{issuer: {AllowedAudiences: "audience"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, err := NewOIDCProvider(context.Background(), marshalOIDCOptions(t, tt.options))

			assert.ErrorContains(t, err, "failed to create OIDC provider for issuer "+issuer)
			assert.Nil(t, provider)
		})
	}
}

// TestOIDCProvider_Authenticate_Legacy tests authentication with legacy configuration.
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

// TestOIDCProvider_Authenticate_PerIssuer tests authentication with per-issuer configuration.
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
		assert.ErrorIs(t, err, errs.ErrForbiddenAudience)
	})
}

// TestOIDCProvider_Authenticate_Errors tests authentication error cases.
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
		assert.ErrorIs(t, err, errs.ErrUnMatchedAuthenticationParamType)
	})

	t.Run("malformed token", func(t *testing.T) {
		_, err := provider.Authenticate(context.Background(), "invalid-token")
		assert.ErrorIs(t, err, errs.ErrMalformedToken)
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
		assert.ErrorIs(t, err, errs.ErrUnknownIssuer)
	})
}

func TestOIDCProviderAuthenticateRejectsUndecodablePayload(t *testing.T) {
	provider := &OIDCProvider{providers: map[string]*ProviderWithVerifier{}}

	_, err := provider.Authenticate(context.Background(), "header.!not-base64!.signature")

	assert.Error(t, err)
}

func TestOIDCProviderAuthenticateRejectsNonJSONPayload(t *testing.T) {
	provider := &OIDCProvider{providers: map[string]*ProviderWithVerifier{}}
	payload := base64.RawURLEncoding.EncodeToString([]byte("not json"))

	_, err := provider.Authenticate(context.Background(), "header."+payload+".signature")

	var syntaxErr *json.SyntaxError
	assert.ErrorAs(t, err, &syntaxErr)
}

func TestOIDCProviderAuthenticateRejectsNonStringIssuer(t *testing.T) {
	provider := &OIDCProvider{providers: map[string]*ProviderWithVerifier{}}
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"iss":1}`))

	_, err := provider.Authenticate(context.Background(), "header."+payload+".signature")

	var typeErr *json.UnmarshalTypeError
	assert.ErrorAs(t, err, &typeErr)
}

func TestOIDCProviderAuthenticateRejectsMissingUserNameClaim(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	issuer := "https://issuer.example.com"
	provider := newStaticKeyOIDCProvider(issuer, &privateKey.PublicKey, DefaultUserNameCalm)
	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": issuer,
		"aud": "audience",
		"exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString(privateKey)
	require.NoError(t, err)

	_, err = provider.Authenticate(context.Background(), token)

	assert.ErrorIs(t, err, errs.ErrUserNameNotFound)
}

func TestOIDCProviderAuthenticateRejectsNonStringUserName(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	issuer := "https://issuer.example.com"
	provider := newStaticKeyOIDCProvider(issuer, &privateKey.PublicKey, "email")
	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss":   issuer,
		"aud":   "audience",
		"sub":   "user",
		"email": 123,
		"exp":   time.Now().Add(time.Hour).Unix(),
	}).SignedString(privateKey)
	require.NoError(t, err)

	_, err = provider.Authenticate(context.Background(), token)

	var typeErr *json.UnmarshalTypeError
	assert.ErrorAs(t, err, &typeErr)
}

// countingKeySet counts the signature checks of the key set it wraps.
type countingKeySet struct {
	oidc.KeySet
	verifications atomic.Int32
}

func (k *countingKeySet) VerifySignature(ctx context.Context, token string) ([]byte, error) {
	k.verifications.Add(1)
	return k.KeySet.VerifySignature(ctx, token)
}

// TestOIDCProvider_Authenticate_Cache tests that a verified token is not
// verified again until its cache entry expires, and that a rejected token is
// not cached.
func TestOIDCProvider_Authenticate_Cache(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	issuer := "https://issuer.example.com"
	keySet := &countingKeySet{KeySet: &oidc.StaticKeySet{PublicKeys: []crypto.PublicKey{&privateKey.PublicKey}}}
	provider := &OIDCProvider{
		providers: map[string]*ProviderWithVerifier{
			issuer: {
				verifier:         oidc.NewVerifier(issuer, keySet, &oidc.Config{SkipClientIDCheck: true}),
				userNameClaim:    DefaultUserNameCalm,
				allowedAudiences: parseAllowedAudiences("audience"),
			},
		},
	}
	signToken := func(audience string, expiry time.Time) string {
		token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, &jwt.RegisteredClaims{
			Audience:  jwt.ClaimStrings{audience},
			ExpiresAt: jwt.NewNumericDate(expiry),
			Issuer:    issuer,
			Subject:   "user",
		}).SignedString(privateKey)
		require.NoError(t, err)
		return token
	}

	t.Run("verified once", func(t *testing.T) {
		keySet.verifications.Store(0)
		token := signToken("audience", time.Now().Add(time.Hour))
		for range 3 {
			userName, err := provider.Authenticate(context.Background(), token)
			require.NoError(t, err)
			assert.Equal(t, "user", userName)
		}
		assert.EqualValues(t, 1, keySet.verifications.Load())
	})

	t.Run("cached until the token expires", func(t *testing.T) {
		expiry := time.Now().Add(30 * time.Second).Truncate(time.Second)
		token := signToken("audience", expiry)
		_, err := provider.Authenticate(context.Background(), token)
		require.NoError(t, err)

		entry := provider.verifiedTokens.entries[token]
		assert.True(t, entry.expiry.Equal(expiry))
	})

	t.Run("rejected token not cached", func(t *testing.T) {
		keySet.verifications.Store(0)
		token := signToken("other-audience", time.Now().Add(time.Hour))
		for range 2 {
			_, err := provider.Authenticate(context.Background(), token)
			assert.ErrorIs(t, err, errs.ErrForbiddenAudience)
		}
		assert.EqualValues(t, 2, keySet.verifications.Load())
	})
}

// TestLoadPublicKeysFromFile tests loading public keys from PEM files.
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

	t.Run("EC public key", func(t *testing.T) {
		privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		pubKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
		require.NoError(t, err)

		keys, err := loadPublicKeysFromFile(writePEMFile(t, &pem.Block{Type: "EC PUBLIC KEY", Bytes: pubKeyBytes}))
		require.NoError(t, err)
		require.Len(t, keys, 1)
		assert.True(t, privateKey.PublicKey.Equal(keys[0]))
	})

	t.Run("certificate", func(t *testing.T) {
		privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		template := &x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject:      pkix.Name{CommonName: "issuer"},
			NotBefore:    time.Now(),
			NotAfter:     time.Now().Add(time.Hour),
		}
		certBytes, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
		require.NoError(t, err)

		keys, err := loadPublicKeysFromFile(writePEMFile(t, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes}))
		require.NoError(t, err)
		require.Len(t, keys, 1)
		assert.True(t, privateKey.PublicKey.Equal(keys[0]))
	})

	t.Run("unrecognized blocks skipped", func(t *testing.T) {
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		keys, err := loadPublicKeysFromFile(writePEMFile(t,
			&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("ignored")},
			&pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&privateKey.PublicKey)}))
		require.NoError(t, err)
		require.Len(t, keys, 1)
		assert.True(t, privateKey.PublicKey.Equal(keys[0]))
	})

	for _, blockType := range []string{"PUBLIC KEY", "RSA PUBLIC KEY", "EC PUBLIC KEY", "CERTIFICATE"} {
		t.Run("invalid "+blockType, func(t *testing.T) {
			_, err := loadPublicKeysFromFile(writePEMFile(t, &pem.Block{Type: blockType, Bytes: []byte("garbage")}))
			assert.ErrorContains(t, err, "failed to parse "+blockType+" block")
		})
	}

	t.Run("empty file", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test-empty-*.pem")
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())
		tempFile.Close()

		_, err = loadPublicKeysFromFile(tempFile.Name())
		assert.ErrorIs(t, err, errs.ErrNoPublicKeysFound)
	})
}

func TestSetupPerIssuerProvidersDefaultsUserNameClaim(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	pubKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)
	issuer := "https://issuer.example.com"
	provider := &OIDCProvider{providers: map[string]*ProviderWithVerifier{}}

	err = setupPerIssuerProviders(context.Background(), provider, map[string]IssuerConfig{
		issuer: {
			AllowedAudiences: "audience",
			StaticKeyFile:    writePEMFile(t, &pem.Block{Type: "PUBLIC KEY", Bytes: pubKeyBytes}),
		},
	})

	require.NoError(t, err)
	require.Contains(t, provider.providers, issuer)
	assert.Equal(t, DefaultUserNameCalm, provider.providers[issuer].userNameClaim)
}

func newStaticKeyOIDCProvider(issuer string, publicKey crypto.PublicKey, userNameClaim string) *OIDCProvider {
	keySet := &oidc.StaticKeySet{PublicKeys: []crypto.PublicKey{publicKey}}
	return &OIDCProvider{
		providers: map[string]*ProviderWithVerifier{
			issuer: {
				verifier:         oidc.NewVerifier(issuer, keySet, &oidc.Config{SkipClientIDCheck: true}),
				userNameClaim:    userNameClaim,
				allowedAudiences: parseAllowedAudiences("audience"),
			},
		},
	}
}

// newNotFoundIssuer returns the URL of an issuer that serves no discovery document.
func newNotFoundIssuer(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	return server.URL
}

func marshalOIDCOptions(t *testing.T, options OIDCOptions) string {
	t.Helper()
	params, err := json.Marshal(options)
	require.NoError(t, err)
	return string(params)
}

func writePEMFile(t *testing.T, blocks ...*pem.Block) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "keys.pem")
	var data []byte
	for _, block := range blocks {
		data = append(data, pem.EncodeToMemory(block)...)
	}
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}
