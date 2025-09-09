package server

import (
	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/proto"
)

type SnapshotController interface {
	Snapshot(request proto.SnapshotRequest, callback concurrent.StreamCallback[proto.SnapshotResponse])
}
