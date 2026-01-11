package option

import (
	"fmt"
	"time"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/security"
	"github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"
	"go.uber.org/multierr"
)

type PublicServerOptions struct {
	BindAddress string              `yaml:"bindAddress" json:"bindAddress"`
	Auth        auth.Options        `yaml:"auth" json:"auth"`
	TLS         security.TLSOptions `yaml:"tls" json:"tls"`
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
	BindAddress string              `yaml:"bindAddress" json:"bindAddress"`
	TLS         security.TLSOptions `yaml:"tls" json:"tls"`
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
	Public   PublicServerOptions   `yaml:"public" json:"public"`
	Internal InternalServerOptions `yaml:"internal" json:"internal"`
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
	TLS security.TLSOptions `yaml:"tls" json:"tls"`
}

func (ro *ReplicationOptions) WithDefault() {
	ro.TLS.WithDefault()
}

func (ro *ReplicationOptions) Validate() error {
	return ro.TLS.Validate()
}

type WALOptions struct {
	Dir       string        `yaml:"dir" json:"dir"`
	Sync      *bool         `yaml:"sync" json:"sync"`
	Retention time.Duration `yaml:"retention" json:"retention"`
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
		wo.Retention = time.Hour * 1
	}
}

func (wo *WALOptions) Validate() error {
	return nil
}

type DatabaseOptions struct {
	Dir             string `yaml:"dir" json:"dir"`
	ReadCacheSizeMB int64  `yaml:"readCacheSizeMB" json:"readCacheSizeMB"`
}

func (do *DatabaseOptions) WithDefault() {
	if do.Dir == "" {
		do.Dir = "./data/db"
	}
	if do.ReadCacheSizeMB == 0 {
		do.ReadCacheSizeMB = 100
	}
}
func (do *DatabaseOptions) Validate() error {
	return nil
}

type NotificationOptions struct {
	Retention time.Duration `yaml:"retention" json:"retention"`
}

func (no *NotificationOptions) WithDefault() {
	no.Retention = time.Hour * 1
}
func (no *NotificationOptions) Validate() error {
	return nil
}

type StorageOptions struct {
	WAL          WALOptions          `yaml:"wal" json:"wal"`
	Database     DatabaseOptions     `yaml:"database" json:"database"`
	Notification NotificationOptions `yaml:"notification" json:"notification"`
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
	Server        ServerOptions               `yaml:"server" json:"server"`
	Replication   ReplicationOptions          `yaml:"replication" json:"replication"`
	Storage       StorageOptions              `yaml:"storage" json:"storage"`
	Observability option.ObservabilityOptions `yaml:"observability" json:"observability"`
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
