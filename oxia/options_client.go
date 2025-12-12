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

package oxia

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/oxia/auth"
)

const (
	DefaultBatchLinger         = 5 * time.Millisecond
	DefaultMaxRequestsPerBatch = 1000
	DefaultMaxBatchSize        = 128 * 1024
	DefaultRequestTimeout      = 30 * time.Second
	DefaultSessionTimeout      = 15 * time.Second
	DefaultNamespace           = constant.DefaultNamespace
)

var (
	ErrInvalidOptionBatchLinger         = errors.New("BatchLinger must be greater than or equal to zero")
	ErrInvalidOptionMaxRequestsPerBatch = errors.New("MaxRequestsPerBatch must be greater than zero")
	ErrInvalidOptionMaxBatchSize        = errors.New("MaxBatchSize must be greater than zero")
	ErrInvalidOptionRequestTimeout      = errors.New("RequestTimeout must be greater than zero")
	ErrInvalidOptionSessionTimeout      = errors.New("SessionTimeout must be greater than zero")
	ErrInvalidOptionIdentity            = errors.New("Identity must be non-empty")
	ErrInvalidOptionNamespace           = errors.New("Namespace cannot be empty")
	ErrInvalidOptionTLS                 = errors.New("Tls cannot be empty")
	ErrInvalidOptionAuthentication      = errors.New("Authentication cannot be empty")
)

// clientOptions contains options for the Oxia client.
type clientOptions struct {
	serviceAddress         string
	namespace              string
	batchLinger            time.Duration
	maxRequestsPerBatch    int
	maxBatchSize           int
	requestTimeout         time.Duration
	meterProvider          metric.MeterProvider
	sessionTimeout         time.Duration
	identity               string
	tls                    *tls.Config
	authentication         auth.Authentication
	sessionKeepAliveTicker time.Duration
	disableIPv6            bool
}

func defaultIdentity() string {
	return uuid.NewString()
}

// RequestTimeout defines how long the client will wait for responses before cancelling the request and failing
// the request.
func (o clientOptions) RequestTimeout() time.Duration {
	return o.requestTimeout
}

// ClientOption is an interface for applying Oxia client options.
type ClientOption interface {
	// apply is used to set a ClientOption value of a clientOptions.
	apply(option clientOptions) (clientOptions, error)
}

func newClientOptions(serviceAddress string, opts ...ClientOption) (clientOptions, error) {
	options := clientOptions{
		serviceAddress:      serviceAddress,
		namespace:           constant.DefaultNamespace,
		batchLinger:         DefaultBatchLinger,
		maxRequestsPerBatch: DefaultMaxRequestsPerBatch,
		maxBatchSize:        DefaultMaxBatchSize,
		requestTimeout:      DefaultRequestTimeout,
		meterProvider:       noop.NewMeterProvider(),
		sessionTimeout:      DefaultSessionTimeout,
		identity:            defaultIdentity(),
	}
	var errs error
	var err error
	for _, o := range opts {
		options, err = o.apply(options)
		if err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	// ======== computed options =========
	defaultTicker := options.sessionTimeout / 10
	if defaultTicker > options.sessionKeepAliveTicker {
		options.sessionKeepAliveTicker = defaultTicker
	}
	return options, errs
}

type clientOptionFunc func(clientOptions) (clientOptions, error)

func (f clientOptionFunc) apply(c clientOptions) (clientOptions, error) {
	return f(c)
}

// WithNamespace set the Oxia namespace to be used for this client.
// If not set, the client will be using the `default` namespace.
func WithNamespace(namespace string) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if namespace == "" {
			return options, ErrInvalidOptionNamespace
		}
		options.namespace = namespace
		return options, nil
	})
}

// WithBatchLinger defines how long the batcher will wait before sending a batched request. The value must be greater
// than or equal to zero. A value of zero will disable linger, effectively disabling batching.
func WithBatchLinger(batchLinger time.Duration) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if batchLinger < 0 {
			return options, ErrInvalidOptionBatchLinger
		}
		options.batchLinger = batchLinger
		return options, nil
	})
}

// WithMaxRequestsPerBatch defines how many individual requests a batch can contain before the batched request is sent.
// The value must be greater than zero. A value of one will effectively disable batching.
func WithMaxRequestsPerBatch(maxRequestsPerBatch int) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if maxRequestsPerBatch <= 0 {
			return options, ErrInvalidOptionMaxRequestsPerBatch
		}
		options.maxRequestsPerBatch = maxRequestsPerBatch
		return options, nil
	})
}

func WithRequestTimeout(requestTimeout time.Duration) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if requestTimeout <= 0 {
			return options, ErrInvalidOptionRequestTimeout
		}
		options.requestTimeout = requestTimeout
		return options, nil
	})
}

func WithMeterProvider(meterProvider metric.MeterProvider) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if meterProvider == nil {
			options.meterProvider = noop.NewMeterProvider()
		} else {
			options.meterProvider = meterProvider
		}
		return options, nil
	})
}

// WithGlobalMeterProvider instructs the Oxia client to use the global OpenTelemetry MeterProvider.
func WithGlobalMeterProvider() ClientOption {
	return WithMeterProvider(otel.GetMeterProvider())
}

// WithSessionTimeout specifies the session timeout to.
func WithSessionTimeout(sessionTimeout time.Duration) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if sessionTimeout <= 0 {
			return options, ErrInvalidOptionSessionTimeout
		}
		options.sessionTimeout = sessionTimeout
		return options, nil
	})
}

func WithIdentity(identity string) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if identity == "" {
			return options, ErrInvalidOptionIdentity
		}
		options.identity = identity
		return options, nil
	})
}

func WithTLS(tlsConf *tls.Config) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if tlsConf == nil {
			return options, ErrInvalidOptionTLS
		}
		options.tls = tlsConf
		return options, nil
	})
}

// WithCACertFile loads a CA certificate from a file and adds it to the client's trusted CA pool.
// This is useful when connecting to servers with certificates signed by a custom CA.
// If a TLS config already exists (e.g., from WithTLS), the CA cert will be merged into the existing
// RootCAs pool. If no TLS config exists, a new one will be created with the CA cert.
func WithCACertFile(caCertPath string) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		caCert, err := os.ReadFile(caCertPath)
		if err != nil {
			return options, errors.Wrapf(err, "failed to read CA certificate from %s", caCertPath)
		}

		// Parse the CA certificate
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return options, errors.Errorf("failed to parse CA certificate from %s", caCertPath)
		}

		// Merge with existing TLS config if present
		if options.tls != nil {
			// Clone the existing TLS config
			tlsConfig := options.tls.Clone()

			// Merge the CA cert into the existing RootCAs pool
			if tlsConfig.RootCAs != nil {
				// Append to existing pool
				if !tlsConfig.RootCAs.AppendCertsFromPEM(caCert) {
					return options, errors.Errorf("failed to add CA certificate to existing RootCAs pool")
				}
			} else {
				// No existing RootCAs, try to get system cert pool and add our cert
				systemPool, err := x509.SystemCertPool()
				if err != nil {
					// Fallback to new pool if system pool unavailable
					tlsConfig.RootCAs = caCertPool
				} else {
					// Add to system pool
					if !systemPool.AppendCertsFromPEM(caCert) {
						return options, errors.Errorf("failed to add CA certificate to system cert pool")
					}
					tlsConfig.RootCAs = systemPool
				}
			}

			// Ensure MinVersion is set if not already specified
			if tlsConfig.MinVersion == 0 {
				tlsConfig.MinVersion = tls.VersionTLS12
			}

			options.tls = tlsConfig
		} else {
			// No existing TLS config, create a new one
			options.tls = &tls.Config{
				RootCAs:    caCertPool,
				MinVersion: tls.VersionTLS12,
			}
		}

		return options, nil
	})
}

// WithDisableIPv6 disables IPv6 resolution, forcing the client to use only IPv4 addresses.
func WithDisableIPv6() ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		options.disableIPv6 = true
		return options, nil
	})
}

func WithAuthentication(authentication auth.Authentication) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		if authentication == nil {
			return options, ErrInvalidOptionAuthentication
		}
		options.authentication = authentication
		return options, nil
	})
}

// WithSessionKeepAliveTicker is an internal API used to control the duration
// of the session keep-alive ticker. This is for experimental use only.
func withSessionKeepAliveTicker(ticker time.Duration) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		options.sessionKeepAliveTicker = ticker
		return options, nil
	})
}
