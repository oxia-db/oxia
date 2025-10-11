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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewClientConfig(t *testing.T) {
	options, err := newClientOptions("serviceAddress")
	assert.NoError(t, err)

	assert.Equal(t, "serviceAddress", options.serviceAddress)
	assert.Equal(t, DefaultBatchLinger, options.batchLinger)
	assert.Equal(t, DefaultMaxRequestsPerBatch, options.maxRequestsPerBatch)
	assert.Equal(t, DefaultRequestTimeout, options.requestTimeout)
}

func TestWithBatchLinger(t *testing.T) {
	for _, item := range []struct {
		batchLinger         time.Duration
		expectedBatchLinger time.Duration
		expectedErr         error
	}{
		{-1, DefaultBatchLinger, ErrInvalidOptionBatchLinger},
		{0, 0, nil},
		{1, 1, nil},
	} {
		options, err := newClientOptions("serviceAddress", WithBatchLinger(item.batchLinger))
		assert.Equal(t, item.expectedBatchLinger, options.batchLinger)
		assert.ErrorIs(t, err, item.expectedErr)
	}
}

func TestWithMaxRequestsPerBatch(t *testing.T) {
	for _, item := range []struct {
		maxRequestsPerBatch         int
		expectedMaxRequestsPerBatch int
		expectedErr                 error
	}{
		{-1, DefaultMaxRequestsPerBatch, ErrInvalidOptionMaxRequestsPerBatch},
		{0, DefaultMaxRequestsPerBatch, ErrInvalidOptionMaxRequestsPerBatch},
		{1, 1, nil},
	} {
		options, err := newClientOptions("serviceAddress", WithMaxRequestsPerBatch(item.maxRequestsPerBatch))
		assert.Equal(t, item.expectedMaxRequestsPerBatch, options.maxRequestsPerBatch)
		assert.ErrorIs(t, err, item.expectedErr)
	}
}

func TestWithRequestTimeout(t *testing.T) {
	for _, item := range []struct {
		requestTimeout         time.Duration
		expectedRequestTimeout time.Duration
		expectedErr            error
	}{
		{-1, DefaultRequestTimeout, ErrInvalidOptionRequestTimeout},
		{0, DefaultRequestTimeout, ErrInvalidOptionRequestTimeout},
		{1, 1, nil},
	} {
		options, err := newClientOptions("serviceAddress", WithRequestTimeout(item.requestTimeout))
		assert.Equal(t, item.expectedRequestTimeout, options.RequestTimeout())
		assert.ErrorIs(t, err, item.expectedErr)
	}
}
