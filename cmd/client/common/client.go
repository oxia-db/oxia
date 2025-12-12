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

//revive:disable-next-line:var-naming
package common

import (
	"time"

	"github.com/oxia-db/oxia/oxia"
)

var (
	Config       = ClientConfig{}
	MockedClient *MockClient
)

type ClientConfig struct {
	ServiceAddr    string
	Namespace      string
	RequestTimeout time.Duration
	CACertFile     string
	DisableIPv6    bool
}

func (ClientConfig) NewClient() (oxia.SyncClient, error) {
	if MockedClient != nil {
		return MockedClient, nil
	}

	opts := []oxia.ClientOption{
		oxia.WithRequestTimeout(Config.RequestTimeout),
		oxia.WithNamespace(Config.Namespace),
	}

	if Config.CACertFile != "" {
		opts = append(opts, oxia.WithCACertFile(Config.CACertFile))
	}

	if Config.DisableIPv6 {
		opts = append(opts, oxia.WithDisableIPv6())
	}

	return oxia.NewSyncClient(Config.ServiceAddr, opts...)
}
