package conf

import (
	"crypto/tls"
	"time"

	"github.com/oxia-db/oxia/node/auth"
)

type Config struct {
	PublicServiceAddr   string
	InternalServiceAddr string
	PeerTLS             *tls.Config
	ServerTLS           *tls.Config
	InternalServerTLS   *tls.Config
	MetricsServiceAddr  string

	AuthOptions auth.Options

	DataDir string
	WalDir  string

	WalRetentionTime           time.Duration
	WalSyncData                bool
	NotificationsRetentionTime time.Duration

	DbBlockCacheMB int64
}
