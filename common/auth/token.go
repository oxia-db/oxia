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
)

type tokenAuthentication struct {
	requireTransportSecurity bool
	tokenGetFunc             func() string
}

func (tokenAuth *tokenAuthentication) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	token := tokenAuth.tokenGetFunc()
	return map[string]string{
		"authorization": "Bearer" + " " + token,
	}, nil
}

func (tokenAuth *tokenAuthentication) RequireTransportSecurity() bool {
	return tokenAuth.requireTransportSecurity
}

func NewTokenAuthenticationWithFunc(tokenGetFunc func() string, requireTransportSecurity bool) Authentication {
	return &tokenAuthentication{tokenGetFunc: tokenGetFunc, requireTransportSecurity: requireTransportSecurity}
}

func NewTokenAuthenticationWithToken(token string, requireTransportSecurity bool) Authentication {
	return NewTokenAuthenticationWithFunc(func() string {
		return token
	}, requireTransportSecurity)
}
