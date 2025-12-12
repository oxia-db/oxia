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

package rpc

import (
	"crypto/tls"
	"crypto/x509"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientPool_GetActualAddress(t *testing.T) {
	pool := NewClientPool(nil, nil)
	poolInstance := pool.(*clientPool)

	address := poolInstance.getActualAddress("tls://xxxxaa:6648")
	assert.Equal(t, "passthrough:///xxxxaa:6648", address)

	actualAddress := poolInstance.getActualAddress("xxxxaaa:6649")
	assert.Equal(t, "passthrough:///xxxxaaa:6649", actualAddress)
}

func TestClientPool_GetTransportCredential(t *testing.T) {
	pool := NewClientPool(nil, nil)
	poolInstance := pool.(*clientPool)

	credential := poolInstance.getTransportCredential("tls://xxxxaa:6648")
	assert.Equal(t, "tls", credential.Info().SecurityProtocol)

	credential = poolInstance.getTransportCredential("xxxxaaa:6649")
	assert.Equal(t, "insecure", credential.Info().SecurityProtocol)
}

func TestClientPool_GetAminRpc(t *testing.T) {
	pool := NewClientPool(nil, nil)
	poolInstance := pool.(*clientPool)

	// This will fail to connect, but we can test that the method exists and returns the right type
	_, err := poolInstance.GetAminRpc("invalid:1234")
	// We expect an error since we can't actually connect, but the method should exist
	assert.Error(t, err)
}

func TestNewClientPoolWithOptions(t *testing.T) {
	t.Run("with disableIPv6 enabled", func(t *testing.T) {
		pool := NewClientPoolWithOptions(ClientPoolOptions{
			TLS:            nil,
			Authentication: nil,
			DisableIPv6:    true,
		})
		poolInstance := pool.(*clientPool)
		assert.True(t, poolInstance.disableIPv6)
	})

	t.Run("with disableIPv6 disabled", func(t *testing.T) {
		pool := NewClientPoolWithOptions(ClientPoolOptions{
			TLS:            nil,
			Authentication: nil,
			DisableIPv6:    false,
		})
		poolInstance := pool.(*clientPool)
		assert.False(t, poolInstance.disableIPv6)
	})

	t.Run("with custom TLS config", func(t *testing.T) {
		tlsConfig := &tls.Config{
			ServerName: "test.example.com",
		}
		pool := NewClientPoolWithOptions(ClientPoolOptions{
			TLS:            tlsConfig,
			Authentication: nil,
			DisableIPv6:    false,
		})
		poolInstance := pool.(*clientPool)
		assert.Equal(t, tlsConfig, poolInstance.tls)
	})
}

func TestClientPool_GetHostname(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		expected string
	}{
		{
			name:     "tls address with port",
			target:   "tls://example.com:6648",
			expected: "example.com",
		},
		{
			name:     "tls address without port",
			target:   "tls://example.com",
			expected: "example.com",
		},
		{
			name:     "non-tls address with port",
			target:   "example.com:6648",
			expected: "example.com",
		},
		{
			name:     "non-tls address without port",
			target:   "example.com",
			expected: "example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetHostname(tt.target)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClientPool_GetTransportCredentialWithCustomCA(t *testing.T) {
	// Create a custom cert pool
	certPool := x509.NewCertPool()
	tlsConfig := &tls.Config{
		RootCAs: certPool,
	}

	poolWithCustomCA := NewClientPoolWithOptions(ClientPoolOptions{
		TLS:            tlsConfig,
		Authentication: nil,
		DisableIPv6:    false,
	})
	poolWithCustomCAInstance := poolWithCustomCA.(*clientPool)

	credential := poolWithCustomCAInstance.getTransportCredential("tls://example.com:6648")
	assert.Equal(t, "tls", credential.Info().SecurityProtocol)
}

func TestClientPool_GetTransportCredentialWithSystemCertPool(t *testing.T) {
	pool := NewClientPool(nil, nil)
	poolInstance := pool.(*clientPool)

	credential := poolInstance.getTransportCredential("tls://example.com:6648")
	assert.Equal(t, "tls", credential.Info().SecurityProtocol)
}
