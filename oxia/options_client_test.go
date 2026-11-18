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

package oxia

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/oxia-db/oxia/common/auth"
)

func TestWithMaxWriteBatchesInFlight(t *testing.T) {
	options, err := newClientOptions("localhost:6648")
	require.NoError(t, err)
	assert.Equal(t, DefaultMaxWriteBatchesInFlight, options.maxWriteBatchesInFlight)

	options, err = newClientOptions("localhost:6648", WithMaxWriteBatchesInFlight(0))
	require.NoError(t, err)
	assert.Equal(t, 0, options.maxWriteBatchesInFlight, "zero sends each write batch once the previous one completed")

	_, err = newClientOptions("localhost:6648", WithMaxWriteBatchesInFlight(-1))
	assert.ErrorIs(t, err, ErrInvalidOptionMaxWriteBatchesInFlight)
}

func TestWithMaxBatchSize(t *testing.T) {
	options, err := newClientOptions("localhost:6648", WithMaxBatchSize(1024))
	require.NoError(t, err)
	assert.Equal(t, 1024, options.maxBatchSize)

	_, err = newClientOptions("localhost:6648", WithMaxBatchSize(0))
	assert.ErrorIs(t, err, ErrInvalidOptionMaxBatchSize)
}

func TestWithDialResolver(t *testing.T) {
	r := &mockResolver{scheme: "test"}

	options, err := newClientOptions("localhost:6648",
		WithDialResolver(r),
	)
	require.NoError(t, err)
	assert.Equal(t, r, options.resolver)
}

func TestWithDialResolver_NotSet(t *testing.T) {
	options, err := newClientOptions("localhost:6648")
	require.NoError(t, err)
	assert.Nil(t, options.resolver)
}

func TestWithDialResolver_Nil(t *testing.T) {
	_, err := newClientOptions("localhost:6648", WithDialResolver(nil))
	assert.ErrorContains(t, err, "must not be nil")
}

func TestWithDialResolver_EmptyScheme(t *testing.T) {
	_, err := newClientOptions("localhost:6648", WithDialResolver(&mockResolver{scheme: ""}))
	assert.ErrorContains(t, err, "must not be empty")
}

func TestWithNamespace(t *testing.T) {
	options, err := newClientOptions("localhost:6648", WithNamespace("ns"))
	require.NoError(t, err)
	assert.Equal(t, "ns", options.namespace)

	options, err = newClientOptions("localhost:6648", WithNamespace(""))
	assert.ErrorIs(t, err, ErrInvalidOptionNamespace)
	assert.Equal(t, DefaultNamespace, options.namespace)
}

func TestWithMeterProvider(t *testing.T) {
	meterProvider := sdkmetric.NewMeterProvider()
	options, err := newClientOptions("localhost:6648", WithMeterProvider(meterProvider))
	require.NoError(t, err)
	assert.Same(t, meterProvider, options.meterProvider)

	// Without a meter provider, the client records no metrics
	options, err = newClientOptions("localhost:6648", WithMeterProvider(nil))
	require.NoError(t, err)
	assert.IsType(t, noop.MeterProvider{}, options.meterProvider)
}

func TestWithGlobalMeterProvider(t *testing.T) {
	options, err := newClientOptions("localhost:6648", WithGlobalMeterProvider())
	require.NoError(t, err)
	assert.Equal(t, otel.GetMeterProvider(), options.meterProvider)
}

func TestWithSessionTimeout(t *testing.T) {
	options, err := newClientOptions("localhost:6648", WithSessionTimeout(20*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 20*time.Second, options.sessionTimeout)
	// The session keep-alive defaults to a tenth of its timeout
	assert.Equal(t, 2*time.Second, options.sessionKeepAliveTicker)

	options, err = newClientOptions("localhost:6648", WithSessionTimeout(0))
	assert.ErrorIs(t, err, ErrInvalidOptionSessionTimeout)
	assert.Equal(t, DefaultSessionTimeout, options.sessionTimeout)
}

func TestWithIdentity(t *testing.T) {
	options, err := newClientOptions("localhost:6648", WithIdentity("client-1"))
	require.NoError(t, err)
	assert.Equal(t, "client-1", options.identity)

	_, err = newClientOptions("localhost:6648", WithIdentity(""))
	assert.ErrorIs(t, err, ErrInvalidOptionIdentity)
}

func TestWithTLS(t *testing.T) {
	tlsConf := &tls.Config{MinVersion: tls.VersionTLS13}
	options, err := newClientOptions("localhost:6648", WithTLS(tlsConf))
	require.NoError(t, err)
	assert.Same(t, tlsConf, options.tls)

	_, err = newClientOptions("localhost:6648", WithTLS(nil))
	assert.ErrorIs(t, err, ErrInvalidOptionTLS)
}

func TestWithAuthentication(t *testing.T) {
	authentication := &mockAuthentication{}
	options, err := newClientOptions("localhost:6648", WithAuthentication(authentication))
	require.NoError(t, err)
	assert.Same(t, authentication, options.authentication)

	_, err = newClientOptions("localhost:6648", WithAuthentication(nil))
	assert.ErrorIs(t, err, ErrInvalidOptionAuthentication)
}

func TestWithSessionKeepAliveTicker(t *testing.T) {
	options, err := newClientOptions("localhost:6648", WithSessionKeepAliveTicker(5*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, options.sessionKeepAliveTicker)
}

// The errors of all the invalid options are reported.
func TestNewClientOptionsReportsAllErrors(t *testing.T) {
	_, err := newClientOptions("localhost:6648", WithIdentity(""), WithTLS(nil))
	assert.ErrorIs(t, err, ErrInvalidOptionIdentity)
	assert.ErrorIs(t, err, ErrInvalidOptionTLS)
}

type mockResolver struct {
	scheme string
}

func (r *mockResolver) Scheme() string                     { return r.scheme }
func (r *mockResolver) Resolve(_ string, _ AddressUpdater) {}

var _ auth.Authentication = (*mockAuthentication)(nil)

type mockAuthentication struct{}

func (*mockAuthentication) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{}, nil
}

func (*mockAuthentication) RequireTransportSecurity() bool { return false }
