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

	"github.com/oxia-db/oxia/cmd/common/clientauth"
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
	Auth           clientauth.Config
}

func (config ClientConfig) NewClient() (oxia.SyncClient, error) {
	if MockedClient != nil {
		return MockedClient, nil
	}

	options := []oxia.ClientOption{
		oxia.WithRequestTimeout(config.RequestTimeout),
		oxia.WithNamespace(config.Namespace),
	}
	if config.Auth.Enabled() {
		authentication, err := config.Auth.GetAuthentication()
		if err != nil {
			return nil, err
		}
		options = append(options, oxia.WithAuthentication(authentication))
	}

	return oxia.NewSyncClient(config.ServiceAddr, options...)
}
