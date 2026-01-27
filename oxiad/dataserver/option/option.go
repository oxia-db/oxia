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

package option

import (
	"fmt"
	"time"

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/security"
	"github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"
)

type PublicServerOptions struct {
	BindAddress string              `yaml:"bindAddress" json:"bindAddress" jsonschema:"description=Bind address for the public API server,example=0.0.0.0:6648,format=hostname"`
	Auth        auth.Options        `yaml:"auth,omitempty" json:"auth,omitempty" jsonschema:"description=Authentication configuration for the public API"`
	TLS         security.TLSOptions `yaml:"tls,omitempty" json:"tls,omitempty" jsonschema:"description=TLS configuration for securing public API connections"`
}

func (pso *PublicServerOptions) WithDefault() {
	if pso.BindAddress == "" {
		pso.BindAddress = fmt.Sprintf("0.0.0.0:%d", constant.DefaultPublicPort)
	}
	pso.Auth.WithDefault()
	pso.TLS.WithDefault()
}

func (pso *PublicServerOptions) Validate() error {
	return multierr.Combine(
		pso.Auth.Validate(),
		pso.TLS.Validate(),
	)
}

type InternalServerOptions struct {
	BindAddress string              `yaml:"bindAddress" json:"bindAddress" jsonschema:"description=Bind address for internal server-to-server communication,example=0.0.0.0:6649,format=hostname"`
	TLS         security.TLSOptions `yaml:"tls,omitempty" json:"tls,omitempty" jsonschema:"description=TLS configuration for securing internal cluster communication"`
}

func (iso *InternalServerOptions) WithDefault() {
	if iso.BindAddress == "" {
		iso.BindAddress = fmt.Sprintf("0.0.0.0:%d", constant.DefaultInternalPort)
	}
	iso.TLS.WithDefault()
}

func (iso *InternalServerOptions) Validate() error {
	return multierr.Combine(
		iso.TLS.Validate(),
	)
}

type ServerOptions struct {
	Public   PublicServerOptions   `yaml:"public" json:"public" jsonschema:"description=Public server configuration for client-facing API endpoints"`
	Internal InternalServerOptions `yaml:"internal" json:"internal" jsonschema:"description=Internal server configuration for cluster communication"`
}

func (so *ServerOptions) WithDefault() {
	so.Public.WithDefault()
	so.Internal.WithDefault()
}

func (so *ServerOptions) Validate() error {
	return multierr.Combine(
		so.Public.Validate(),
		so.Internal.Validate(),
	)
}

type ReplicationOptions struct {
	TLS security.TLSOptions `yaml:"tls,omitempty" json:"tls,omitempty" jsonschema:"description=TLS configuration for securing replication traffic between data servers"`
}

func (ro *ReplicationOptions) WithDefault() {
	ro.TLS.WithDefault()
}

func (ro *ReplicationOptions) Validate() error {
	return ro.TLS.Validate()
}

type WALOptions struct {
	Dir       string          `yaml:"dir" json:"dir" jsonschema:"description=Directory path for storing WAL files,example=./data/wal"`
	Sync      *bool           `yaml:"sync,omitempty" json:"sync,omitempty" jsonschema:"description=Enable synchronous WAL writes for durability,default=true"`
	Retention option.Duration `yaml:"retention" json:"retention" jsonschema:"description=Duration to retain WAL entries before cleanup,example=1h,format=duration"`
}

func (wo *WALOptions) IsSyncEnabled() bool {
	if wo.Sync == nil {
		return true
	}
	return *wo.Sync
}

func (wo *WALOptions) WithDefault() {
	if wo.Dir == "" {
		wo.Dir = "./data/wal"
	}
	if wo.Sync == nil {
		wo.Sync = &constant.FlagTrue
	}

	if wo.Retention == 0 {
		wo.Retention = option.Duration(time.Hour * 1)
	}
}

func (*WALOptions) Validate() error {
	return nil
}

type DatabaseOptions struct {
	Dir             string           `yaml:"dir" json:"dir" jsonschema:"description=Directory path for storing database files,example=./data/db"`
	ReadCacheSizeMB int64            `yaml:"readCacheSizeMB,omitempty" json:"readCacheSizeMB,omitempty" jsonschema:"description=Size of read cache in megabytes for performance optimization,default=100,minimum=1"`
	WriteCacheSize  option.BytesSize `yaml:"writeCacheSize,omitempty" json:"writeCacheSize,omitempty" jsonschema:"description=Size of write cache for performance optimization,example=50MB,format=bytes"`
}

func (do *DatabaseOptions) WithDefault() {
	if do.Dir == "" {
		do.Dir = "./data/db"
	}
	if do.ReadCacheSizeMB == 0 {
		do.ReadCacheSizeMB = 100
	}
	if do.WriteCacheSize == 0 {
		do.WriteCacheSize = option.BytesSize(32 * 1024 * 1024)
	}
}
func (*DatabaseOptions) Validate() error {
	return nil
}

type NotificationOptions struct {
	Retention option.Duration `yaml:"retention" json:"retention" jsonschema:"description=Duration to retain notifications before cleanup,example=1h,format=duration"`
}

func (no *NotificationOptions) WithDefault() {
	if no.Retention == 0 {
		no.Retention = option.Duration(time.Hour * 1)
	}
}
func (*NotificationOptions) Validate() error {
	return nil
}

type StorageOptions struct {
	WAL          WALOptions          `yaml:"wal" json:"wal" jsonschema:"description=Write-Ahead Log configuration for durability and recovery"`
	Database     DatabaseOptions     `yaml:"database" json:"database" jsonschema:"description=Database storage configuration for persistent data"`
	Notification NotificationOptions `yaml:"notification" json:"notification" jsonschema:"description=Notification system configuration for change events"`
}

func (so *StorageOptions) WithDefault() {
	so.WAL.WithDefault()
	so.Database.WithDefault()
	so.Notification.WithDefault()
}

func (so *StorageOptions) Validate() error {
	return multierr.Combine(
		so.WAL.Validate(),
		so.Database.Validate(),
		so.Notification.Validate(),
	)
}

type Options struct {
	Server        ServerOptions               `yaml:"server" json:"server" jsonschema:"description=Server configuration for public and internal endpoints"`
	Replication   ReplicationOptions          `yaml:"replication,omitempty" json:"replication,omitempty" jsonschema:"description=Replication configuration for data consistency"`
	Storage       StorageOptions              `yaml:"storage" json:"storage" jsonschema:"description=Storage configuration for WAL, database, and notifications"`
	Observability option.ObservabilityOptions `yaml:"observability" json:"observability" jsonschema:"description=Observability configuration for metrics and tracing"`
}

func (op *Options) WithDefault() {
	op.Server.WithDefault()
	op.Replication.WithDefault()
	op.Storage.WithDefault()
	op.Observability.WithDefault()
}

func (op *Options) Validate() error {
	return multierr.Combine(
		op.Server.Validate(),
		op.Replication.Validate(),
		op.Storage.Validate(),
		op.Observability.Validate(),
	)
}

func NewDefaultOptions() *Options {
	options := Options{}
	options.WithDefault()
	return &options
}
