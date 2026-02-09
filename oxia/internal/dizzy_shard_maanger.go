package internal

import (
	"time"

	"github.com/oxia-db/oxia/common/rpc"
)

var _ ShardManager = &DizzyShardManager{}

type DizzyShardManager struct {
	ShardManager
	lookupAddress string
}

func (d *DizzyShardManager) Leader(int64) string {
	return d.lookupAddress
}

func NewDizzyShardManager(shardStrategy ShardStrategy, clientPool rpc.ClientPool,
	serviceAddress string, namespace string, requestTimeout time.Duration) (ShardManager, error) {
	manager, err := NewShardManager(shardStrategy, clientPool, serviceAddress, namespace, requestTimeout)
	if err != nil {
		return nil, err
	}
	return &DizzyShardManager{manager, serviceAddress}, nil
}
