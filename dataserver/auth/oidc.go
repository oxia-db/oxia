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
	"encoding/base64"
	"encoding/json"
	"net/http"
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
)

type OIDCOptions struct {
	AllowedIssueURLs string `json:"allowedIssueURLs,omitempty"`
	AllowedAudiences string `json:"allowedAudiences,omitempty"`
	UserNameClaim    string `json:"userNameClaim,omitempty"`
}

func (op *OIDCOptions) Validate() error {
	if op.AllowedIssueURLs == "" {
		return ErrEmptyIssueURL
	}
	if op.AllowedAudiences == "" {
		return ErrEmptyAllowedAudiences
	}
	return nil
}

func (op *OIDCOptions) withDefault() {
	if op.UserNameClaim == "" {
		op.UserNameClaim = DefaultUserNameCalm
	}
}

type ProviderWithVerifier struct {
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
}

type OIDCProvider struct {
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
	rawMessage, ok := rawClaims[p.userNameClaim]
	if !ok {
		return "", ErrUserNameNotFound
	}
	var userName string
	if err = json.Unmarshal(rawMessage, &userName); err != nil {
		return "", err
	}

	// any of the client audience in the allowed is passed
	audienceAllowed := false
	audienceArr := idToken.Audience
	for _, audience := range audienceArr {
		if _, ok := p.allowedAudiences[audience]; ok {
			audienceAllowed = true
		}
	}
	if !audienceAllowed {
		return "", ErrForbiddenAudience
	}
	return userName, nil
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
	allowedAudienceMap := map[string]string{}
	allowedAudienceArr := strings.Split(oidcParams.AllowedAudiences, ",")
	for i := range allowedAudienceArr {
		allowedAudience := allowedAudienceArr[i]
		allowedAudienceMap[allowedAudience] = AllowedAudienceDefaultValue
	}
	oidcProvider := &OIDCProvider{
		userNameClaim:    oidcParams.UserNameClaim,
		allowedAudiences: allowedAudienceMap,
		providers:        make(map[string]*ProviderWithVerifier),
	}

	ctx = oidc.ClientContext(ctx, &http.Client{Timeout: 30 * time.Second})
	urlArr := strings.Split(oidcParams.AllowedIssueURLs, ",")
	for i := 0; i < len(urlArr); i++ {
		issueURL := urlArr[i]
		provider, err := oidc.NewProvider(ctx, issueURL)
		if err != nil {
			return nil, err
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
	return oidcProvider, nil
}
