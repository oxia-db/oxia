package option

import (
	"time"

	"github.com/oxia-db/oxia/common/security"
	"github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"
)

type PublicServerOptions struct {
	BindAddress string              `yaml:"bindAddress" json:"bindAddress"`
	Auth        auth.Options        `yaml:"auth" json:"auth"`
	TLS         security.TLSOptions `yaml:"tls" json:"tls"`
}

type InternalServerOptions struct {
	BindAddress string              `yaml:"bindAddress" json:"bindAddress"`
	TLS         security.TLSOptions `yaml:"tls" json:"tls"`
}

type ServerOptions struct {
	Public   PublicServerOptions   `yaml:"public" json:"public"`
	Internal InternalServerOptions `yaml:"internal" json:"internal"`
}

type ReplicationOptions struct {
	TLS security.TLSOptions `yaml:"tls" json:"tls"`
}

type WALOptions struct {
	Dir       string        `yaml:"dir" json:"dir"`
	Sync      bool          `yaml:"sync" json:"sync"`
	Retention time.Duration `yaml:"retention" json:"retention"`
}

type DatabaseOptions struct {
	Dir             string `yaml:"dir" json:"dir"`
	ReadCacheSizeMB int64  `yaml:"readCacheSizeMB" json:"readCacheSizeMB"`
}

type NotificationOptions struct {
	Retention time.Duration `yaml:"retention" json:"retention"`
}

type StorageOptions struct {
	WAL          WALOptions          `yaml:"wal" json:"wal"`
	Database     DatabaseOptions     `yaml:"database" json:"database"`
	Notification NotificationOptions `yaml:"notification" json:"notification"`
}

type Options struct {
	Server        ServerOptions               `yaml:"server" json:"server"`
	Replication   ReplicationOptions          `yaml:"replication" json:"replication"`
	Storage       StorageOptions              `yaml:"storage" json:"storage"`
	Observability option.ObservabilityOptions `yaml:"observability" json:"observability"`
}

func (ob *Options) WithDefault() {
}

func (ob *Options) Validate() error {
	return nil
}
