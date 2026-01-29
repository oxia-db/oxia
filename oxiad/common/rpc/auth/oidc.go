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
	"crypto"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/pkg/errors"
)

const (
	DefaultUserNameCalm         = "sub"
	AllowedAudienceDefaultValue = ""
)

var (
	ErrEmptyIssueURL         = errors.New("empty issue URL")
	ErrEmptyAllowedAudiences = errors.New("empty allowed audiences")
	ErrUnknownIssuer         = errors.New("unknown issuer")
	ErrUserNameNotFound      = errors.New("username not found")
	ErrForbiddenAudience     = errors.New("forbidden audience")
	ErrNoPublicKeysFound     = errors.New("no public keys found in file")
)

// IssuerConfig holds per-issuer OIDC configuration
type IssuerConfig struct {
	AllowedAudiences string `json:"allowedAudiences,omitempty"`
	UserNameClaim    string `json:"userNameClaim,omitempty"`
	StaticKeyFile    string `json:"staticKeyFile,omitempty"`
}

type OIDCOptions struct {
	// Legacy fields for backward compatibility
	AllowedIssueURLs string `json:"allowedIssueURLs,omitempty"`
	AllowedAudiences string `json:"allowedAudiences,omitempty"`
	UserNameClaim    string `json:"userNameClaim,omitempty"`

	// New per-issuer configuration
	Issuers map[string]IssuerConfig `json:"issuers,omitempty"`
}

func (op *OIDCOptions) Validate() error {
	// Support both old and new formats
	if len(op.Issuers) == 0 && op.AllowedIssueURLs == "" {
		return ErrEmptyIssueURL
	}
	// If using legacy format, validate it
	if op.AllowedIssueURLs != "" && op.AllowedAudiences == "" {
		return ErrEmptyAllowedAudiences
	}
	// If using new format, validate each issuer config
	for issuerURL, config := range op.Issuers {
		if issuerURL == "" {
			return ErrEmptyIssueURL
		}
		if config.AllowedAudiences == "" {
			return errors.Wrapf(ErrEmptyAllowedAudiences, "for issuer %s", issuerURL)
		}
	}
	return nil
}

func (op *OIDCOptions) withDefault() {
	if op.UserNameClaim == "" {
		op.UserNameClaim = DefaultUserNameCalm
	}
	// Apply defaults to per-issuer configs
	for issuerURL, config := range op.Issuers {
		if config.UserNameClaim == "" {
			config.UserNameClaim = DefaultUserNameCalm
			op.Issuers[issuerURL] = config
		}
	}
}

type ProviderWithVerifier struct {
	provider         *oidc.Provider
	verifier         *oidc.IDTokenVerifier
	userNameClaim    string
	allowedAudiences map[string]string
}

type OIDCProvider struct {
	// Legacy fields for backward compatibility
	userNameClaim    string
	allowedAudiences map[string]string

	providers map[string]*ProviderWithVerifier
}

func (*OIDCProvider) AcceptParamType() string {
	return ProviderParamTypeToken
}

func (p *OIDCProvider) Authenticate(ctx context.Context, param any) (string, error) {
	token, ok := param.(string)
	if !ok {
		return "", ErrUnMatchedAuthenticationParamType
	}
	tokenParts := strings.Split(token, ".")
	if len(tokenParts) != 3 {
		return "", ErrMalformedToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(tokenParts[1])
	if err != nil {
		return "", err
	}
	unsecureJwtPayload := &struct {
		Issuer string `json:"iss"`
	}{}
	if err = json.Unmarshal(payload, unsecureJwtPayload); err != nil {
		return "", err
	}
	issuer := unsecureJwtPayload.Issuer
	oidcProvider, exist := p.providers[issuer]
	if !exist {
		return "", ErrUnknownIssuer
	}
	idToken, err := oidcProvider.verifier.Verify(ctx, token)
	if err != nil {
		return "", err
	}
	rawClaims := map[string]json.RawMessage{}
	if err = idToken.Claims(&rawClaims); err != nil {
		return "", err
	}

	// Use per-issuer userNameClaim if available, otherwise fall back to global
	userNameClaim := oidcProvider.userNameClaim
	if userNameClaim == "" {
		userNameClaim = p.userNameClaim
	}

	rawMessage, ok := rawClaims[userNameClaim]
	if !ok {
		return "", ErrUserNameNotFound
	}
	var userName string
	if err = json.Unmarshal(rawMessage, &userName); err != nil {
		return "", err
	}

	// Use per-issuer allowedAudiences if available, otherwise fall back to global
	allowedAudiences := oidcProvider.allowedAudiences
	if len(allowedAudiences) == 0 {
		allowedAudiences = p.allowedAudiences
	}

	// any of the client audience in the allowed is passed
	audienceAllowed := false
	audienceArr := idToken.Audience
	for _, audience := range audienceArr {
		if _, ok := allowedAudiences[audience]; ok {
			audienceAllowed = true
		}
	}
	if !audienceAllowed {
		return "", ErrForbiddenAudience
	}
	return userName, nil
}

// loadPublicKeysFromFile loads public keys from a PEM-encoded file.
// The file can contain multiple PEM blocks with public keys or certificates.
func loadPublicKeysFromFile(filePath string) ([]crypto.PublicKey, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read key file")
	}

	var publicKeys []crypto.PublicKey
	rest := data

	for {
		block, remaining := pem.Decode(rest)
		if block == nil {
			break
		}
		rest = remaining

		// Try to parse based on block type
		switch block.Type {
		case "PUBLIC KEY":
			// Standard PKIX format (works for RSA, ECDSA, Ed25519)
			key, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse PUBLIC KEY block")
			}
			publicKeys = append(publicKeys, key)
		case "RSA PUBLIC KEY":
			// PKCS#1 RSA public key
			key, err := x509.ParsePKCS1PublicKey(block.Bytes)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse RSA PUBLIC KEY block")
			}
			publicKeys = append(publicKeys, key)
		case "EC PUBLIC KEY":
			// Try PKIX first for EC keys
			key, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse EC PUBLIC KEY block")
			}
			publicKeys = append(publicKeys, key)
		case "CERTIFICATE":
			// Extract public key from certificate
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse CERTIFICATE block")
			}
			publicKeys = append(publicKeys, cert.PublicKey)
		default:
			// Skip unrecognized block types silently
		}
	}

	if len(publicKeys) == 0 {
		return nil, ErrNoPublicKeysFound
	}

	return publicKeys, nil
}

func NewOIDCProvider(ctx context.Context, jsonParam string) (AuthenticationProvider, error) {
	oidcParams := &OIDCOptions{}
	if err := json.Unmarshal([]byte(jsonParam), oidcParams); err != nil {
		return nil, err
	}
	oidcParams.withDefault()
	if err := oidcParams.Validate(); err != nil {
		return nil, err
	}

	oidcProvider := &OIDCProvider{
		providers: make(map[string]*ProviderWithVerifier),
	}

	ctx = oidc.ClientContext(ctx, &http.Client{Timeout: 30 * time.Second})

	// Handle new per-issuer configuration
	if len(oidcParams.Issuers) > 0 {
		for issuerURL, issuerConfig := range oidcParams.Issuers {
			issuerURL = strings.TrimSpace(issuerURL)

			// Parse allowed audiences for this issuer
			allowedAudienceMap := map[string]string{}
			allowedAudienceArr := strings.Split(issuerConfig.AllowedAudiences, ",")
			for i := range allowedAudienceArr {
				allowedAudience := strings.TrimSpace(allowedAudienceArr[i])
				allowedAudienceMap[allowedAudience] = AllowedAudienceDefaultValue
			}

			userNameClaim := issuerConfig.UserNameClaim
			if userNameClaim == "" {
				userNameClaim = DefaultUserNameCalm
			}

			// Check if there's a static key file for this issuer
			if issuerConfig.StaticKeyFile != "" {
				// Load static keys for this issuer
				publicKeys, err := loadPublicKeysFromFile(issuerConfig.StaticKeyFile)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to load static key file for issuer %s", issuerURL)
				}

				staticKeySet := &oidc.StaticKeySet{PublicKeys: publicKeys}
				config := &oidc.Config{
					SkipClientIDCheck: true,
					Now:               time.Now,
				}

				verifier := oidc.NewVerifier(issuerURL, staticKeySet, config)
				oidcProvider.providers[issuerURL] = &ProviderWithVerifier{
					provider:         nil, // No provider when using static keys
					verifier:         verifier,
					userNameClaim:    userNameClaim,
					allowedAudiences: allowedAudienceMap,
				}
			} else {
				// Use remote JWKS
				provider, err := oidc.NewProvider(ctx, issuerURL)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to create OIDC provider for issuer %s", issuerURL)
				}
				config := &oidc.Config{
					SkipClientIDCheck: true,
					Now:               time.Now,
				}
				verifier := provider.Verifier(config)
				oidcProvider.providers[issuerURL] = &ProviderWithVerifier{
					provider:         provider,
					verifier:         verifier,
					userNameClaim:    userNameClaim,
					allowedAudiences: allowedAudienceMap,
				}
			}
		}
	} else {
		// Handle legacy configuration for backward compatibility
		allowedAudienceMap := map[string]string{}
		allowedAudienceArr := strings.Split(oidcParams.AllowedAudiences, ",")
		for i := range allowedAudienceArr {
			allowedAudience := strings.TrimSpace(allowedAudienceArr[i])
			allowedAudienceMap[allowedAudience] = AllowedAudienceDefaultValue
		}
		oidcProvider.userNameClaim = oidcParams.UserNameClaim
		oidcProvider.allowedAudiences = allowedAudienceMap

		urlArr := strings.Split(oidcParams.AllowedIssueURLs, ",")
		for i := 0; i < len(urlArr); i++ {
			issueURL := strings.TrimSpace(urlArr[i])
			provider, err := oidc.NewProvider(ctx, issueURL)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to create OIDC provider for issuer %s", issueURL)
			}
			config := &oidc.Config{
				SkipClientIDCheck: true,
				Now:               time.Now,
			}
			verifier := provider.Verifier(config)
			oidcProvider.providers[issueURL] = &ProviderWithVerifier{
				provider: provider,
				verifier: verifier,
			}
		}
	}

	return oidcProvider, nil
}
