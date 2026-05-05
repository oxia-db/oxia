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

package clientauth

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Enabled(t *testing.T) {
	assert.False(t, Config{}.Enabled())
	assert.True(t, Config{Token: "token"}.Enabled())
	assert.True(t, Config{TokenFile: "token-file"}.Enabled())
}

func TestConfig_GetAuthenticationWithToken(t *testing.T) {
	authentication, err := Config{Token: " token\n"}.GetAuthentication()
	require.NoError(t, err)

	metadata, err := authentication.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "Bearer token", metadata["authorization"])
	assert.True(t, authentication.RequireTransportSecurity())
}

func TestConfig_GetAuthenticationWithTokenFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("file-token\n"), 0o600))

	authentication, err := Config{TokenFile: tokenFile}.GetAuthentication()
	require.NoError(t, err)

	metadata, err := authentication.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "Bearer file-token", metadata["authorization"])
	assert.True(t, authentication.RequireTransportSecurity())
}

func TestConfig_GetAuthenticationWithoutToken(t *testing.T) {
	_, err := Config{}.GetAuthentication()
	require.ErrorIs(t, err, ErrAuthTokenRequired)
}

func TestConfig_GetAuthenticationWithConflictingTokenSources(t *testing.T) {
	_, err := Config{Token: "token", TokenFile: "token-file"}.GetAuthentication()
	require.ErrorIs(t, err, ErrAuthTokenConflict)
}

func TestConfig_GetAuthenticationWithEmptyToken(t *testing.T) {
	_, err := Config{Token: " \n"}.GetAuthentication()
	require.ErrorIs(t, err, ErrAuthTokenRequired)
}

func TestConfig_GetAuthenticationWithEmptyTokenFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("\n"), 0o600))

	_, err := Config{TokenFile: tokenFile}.GetAuthentication()
	require.ErrorIs(t, err, ErrAuthTokenRequired)
}

func TestConfig_GetAuthenticationWithMissingTokenFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "missing")
	_, err := Config{TokenFile: tokenFile}.GetAuthentication()
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrAuthTokenRequired))
	assert.Contains(t, err.Error(), tokenFile)
}
