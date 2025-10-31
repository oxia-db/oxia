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

	"github.com/pkg/errors"
)

const (
	ProviderOIDC = "oidc"

	ProviderParamTypeToken = "token"
)

var (
	ErrUnsupportedProvider              = errors.New("unsupported authentication provider")
	ErrUnMatchedAuthenticationParamType = errors.New("unmatched authentication parameter type")
	ErrEmptyToken                       = errors.New("empty token")
	ErrMalformedToken                   = errors.New("malformed token")
)

var Disabled = Options{}

type Options struct {
	ProviderName   string
	ProviderParams string
}

func (op *Options) IsEnabled() bool {
	return op != nil && op.ProviderName != ""
}

// todo: add metrics
type AuthenticationProvider interface {
	AcceptParamType() string
	Authenticate(ctx context.Context, param any) (string, error)
}

func NewAuthenticationProvider(ctx context.Context, options Options) (AuthenticationProvider, error) {
	switch options.ProviderName {
	case ProviderOIDC:
		return NewOIDCProvider(ctx, options.ProviderParams)
	default:
		return nil, ErrUnsupportedProvider
	}
}
