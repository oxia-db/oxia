// Copyright 2025 StreamNative, Inc.
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

package config

import (
	"crypto/tls"
	"time"

	"github.com/streamnative/oxia/common/security"
)

type ServerConfig struct {
	PublicServiceAddr   string
	InternalServiceAddr string
	PeerTLS             *tls.Config
	ServerTLS           *tls.Config
	InternalServerTLS   *tls.Config
	MetricsServiceAddr  string

	AuthOptions security.Options

	DataDir string
	WalDir  string

	WalRetentionTime           time.Duration
	WalSyncData                bool
	NotificationsRetentionTime time.Duration

	DbBlockCacheMB int64
}
