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

// Package clientauth turns CLI auth options into Oxia client credentials.
package clientauth

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/oxia-db/oxia/common/auth"
)

var (
	// ErrAuthTokenRequired is returned when auth is enabled without a token value.
	ErrAuthTokenRequired = errors.New("auth token must be configured")
	// ErrAuthTokenConflict is returned when more than one token source is configured.
	ErrAuthTokenConflict = errors.New("only one of auth-token or auth-token-file can be configured")
)

// Config contains CLI authentication options.
type Config struct {
	Token     string
	TokenFile string
}

// Enabled reports whether the CLI auth options should create credentials.
func (c Config) Enabled() bool {
	return c.Token != "" || c.TokenFile != ""
}

// GetAuthentication creates Oxia token credentials from the CLI auth options.
func (c Config) GetAuthentication() (auth.Authentication, error) {
	if c.Token != "" && c.TokenFile != "" {
		return nil, ErrAuthTokenConflict
	}

	token := strings.TrimSpace(c.Token)
	if c.TokenFile != "" {
		data, err := os.ReadFile(c.TokenFile)
		if err != nil {
			return nil, fmt.Errorf("read auth token file %q: %w", c.TokenFile, err)
		}
		token = strings.TrimSpace(string(data))
	}

	if token == "" {
		return nil, ErrAuthTokenRequired
	}

	return auth.NewTokenAuthenticationWithToken(token, true), nil
}
