package coordinator

import (
	"context"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/common"
	"oxia/proto"
	"sync/atomic"
)

// The NodeController takes care of checking the health-status of each node
// and to push all the service discovery updates
type NodeController interface {
	io.Closer
}

type nodeController struct {
	addr                     string
	shardAssignmentsProvider ShardAssignmentsProvider
	clientPool               common.ClientPool
	log                      zerolog.Logger
	closed                   atomic.Bool
	ctx                      context.Context
	cancel                   context.CancelFunc
}

func NewNodeController(addr string, shardAssignmentsProvider ShardAssignmentsProvider, clientPool common.ClientPool) NodeController {
	nc := &nodeController{
		addr:                     addr,
		shardAssignmentsProvider: shardAssignmentsProvider,
		clientPool:               clientPool,
		log: log.With().
			Str("component", "node-controller").
			Str("addr", addr).
			Logger(),
	}

	nc.ctx, nc.cancel = context.WithCancel(context.Background())

	go common.DoWithLabels(map[string]string{
		"oxia": "node-controller",
		"addr": nc.addr,
	}, func() {
		nc.healthCheck()
	})

	go common.DoWithLabels(map[string]string{
		"oxia": "node-controller-send-updates",
		"addr": nc.addr,
	}, func() {
		nc.sendAssignmentsUpdatesWithRetries()
	})
	return nc
}

func (n *nodeController) healthCheck() {
	// TODO: implement node health-check
}

func (n *nodeController) sendAssignmentsUpdatesWithRetries() {
	for !n.closed.Load() {
		err := n.sendAssignmentsUpdates()
		if err != nil {
			n.log.Warn().Err(err).
				Msg("Failed to send assignments updates to storage node")

			// TODO: add backoff logic after failures
			continue
		}
	}
}

func (n *nodeController) sendAssignmentsUpdates() error {
	rpc, err := n.clientPool.GetControlRpc(n.addr)
	if err != nil {
		return err
	}

	stream, err := rpc.ShardAssignment(n.ctx)
	if err != nil {
		return err
	}

	var assignments *proto.ShardAssignmentsResponse
	for !n.closed.Load() {

		assignments = n.shardAssignmentsProvider.WaitForNextUpdate(assignments)
		if assignments == nil {
			continue
		}

		n.log.Debug().
			Interface("assignments", assignments).
			Msg("Sending assignments")

		if err := stream.Send(assignments); err != nil {
			n.log.Debug().Err(err).
				Msg("Failed to send assignments")
			return err
		}
	}

	return nil
}

func (n *nodeController) Close() error {
	n.closed.Store(true)
	n.cancel()
	return nil
}
