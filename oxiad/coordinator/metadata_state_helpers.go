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

package coordinator

import (
	"context"

	gproto "google.golang.org/protobuf/proto"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

func waitForMetadataLease(ctx context.Context, watch *commonoption.Watch[metadatapb.LeaseState]) error {
	state, version := watch.Load()
	for state != metadatapb.LeaseState_LEASE_STATE_HELD {
		var err error
		state, version, err = watch.Wait(ctx, version)
		if err != nil {
			return err
		}
	}
	return nil
}

func cloneClusterState(state *metadatapb.ClusterState) *metadatapb.ClusterState {
	if state == nil {
		return &metadatapb.ClusterState{}
	}
	return gproto.CloneOf(state)
}

func clusterStatusFromProto(state *metadatapb.ClusterState, resolve func(string) (model.Server, bool)) *model.ClusterStatus {
	status := model.NewClusterStatus()
	if state == nil {
		return status
	}

	status.ShardIdGenerator = state.ShardIdGenerator
	// v2 does not persist the legacy round-robin cursor, so keep a stable
	// best-effort seed derived from the shard id generator.
	status.ServerIdx = uint32(state.ShardIdGenerator)

	for namespace, namespaceState := range state.Namespaces {
		namespaceStatus := model.NamespaceStatus{
			Shards: map[int64]model.ShardMetadata{},
		}
		if namespaceState != nil {
			namespaceStatus.ReplicationFactor = namespaceState.ReplicationFactor
			for shardID, shardState := range namespaceState.Shards {
				namespaceStatus.Shards[shardID] = shardStateToModel(shardState, resolve)
			}
		}
		status.Namespaces[namespace] = namespaceStatus
	}
	return status
}

func clusterStatusToProto(status *model.ClusterStatus) *metadatapb.ClusterState {
	state := &metadatapb.ClusterState{
		Namespaces: map[string]*metadatapb.NamespaceState{},
	}
	if status == nil {
		return state
	}

	state.ShardIdGenerator = status.ShardIdGenerator
	for namespace, namespaceStatus := range status.Namespaces {
		protoNamespace := &metadatapb.NamespaceState{
			ReplicationFactor: namespaceStatus.ReplicationFactor,
			Shards:            map[int64]*metadatapb.ShardState{},
		}
		for shardID, shardState := range namespaceStatus.Shards {
			protoNamespace.Shards[shardID] = shardStateFromModel(shardState)
		}
		state.Namespaces[namespace] = protoNamespace
	}
	return state
}

func cloneNamespaceState(state *metadatapb.NamespaceState) *metadatapb.NamespaceState {
	if state == nil {
		return &metadatapb.NamespaceState{}
	}
	return gproto.CloneOf(state)
}

func shardStateToModel(state *metadatapb.ShardState, resolve func(string) (model.Server, bool)) model.ShardMetadata {
	if state == nil {
		return model.ShardMetadata{}
	}

	result := model.ShardMetadata{
		Status:                  shardStatusFromProto(state.Status),
		Term:                    state.Term,
		Leader:                  serverPtrFromIdentifier(state.Leader, resolve),
		Ensemble:                serversFromIdentifiers(state.Ensemble, resolve),
		RemovedNodes:            serversFromIdentifiers(state.RemovedNodes, resolve),
		PendingDeleteShardNodes: serversFromIdentifiers(state.PendingDeleteShardNodes, resolve),
	}
	if state.Int32HashRange != nil {
		result.Int32HashRange = model.Int32HashRange{
			Min: state.Int32HashRange.Min,
			Max: state.Int32HashRange.Max,
		}
	}
	if state.Split != nil {
		result.Split = &model.SplitMetadata{
			Phase:                 splitPhaseFromProto(state.Split.Phase),
			ChildShardIDs:         append([]int64(nil), state.Split.ChildShardIds...),
			SplitPoint:            state.Split.SplitPoint,
			SnapshotOffset:        state.Split.GetSnapshotOffset(),
			ParentTermAtBootstrap: state.Split.GetParentTermAtBootstrap(),
			ChildLeadersAtBootstrap: func() map[int64]string {
				if len(state.Split.ChildLeadersAtBootstrap) == 0 {
					return nil
				}
				cloned := make(map[int64]string, len(state.Split.ChildLeadersAtBootstrap))
				for shardID, leader := range state.Split.ChildLeadersAtBootstrap {
					cloned[shardID] = leader
				}
				return cloned
			}(),
		}
		if state.Split.ParentShardId != nil {
			result.Split.ParentShardId = state.Split.GetParentShardId()
		}
	}
	return result
}

func shardStateFromModel(metadata model.ShardMetadata) *metadatapb.ShardState {
	record := &metadatapb.ShardState{
		Status:                  shardStatusToProto(metadata.Status),
		Term:                    metadata.Term,
		Leader:                  serverIdentifier(metadata.Leader),
		Ensemble:                serverIdentifiers(metadata.Ensemble),
		RemovedNodes:            serverIdentifiers(metadata.RemovedNodes),
		PendingDeleteShardNodes: serverIdentifiers(metadata.PendingDeleteShardNodes),
		Int32HashRange: &metadatapb.Int32HashRange{
			Min: metadata.Int32HashRange.Min,
			Max: metadata.Int32HashRange.Max,
		},
	}
	if metadata.Split != nil {
		record.Split = &metadatapb.ShardSplittingState{
			Phase:                 splitPhaseToProto(metadata.Split.Phase),
			ChildShardIds:         append([]int64(nil), metadata.Split.ChildShardIDs...),
			SplitPoint:            metadata.Split.SplitPoint,
			SnapshotOffset:        valuePtr(metadata.Split.SnapshotOffset),
			ParentTermAtBootstrap: valuePtr(metadata.Split.ParentTermAtBootstrap),
			ChildLeadersAtBootstrap: func() map[int64]string {
				if len(metadata.Split.ChildLeadersAtBootstrap) == 0 {
					return nil
				}
				cloned := make(map[int64]string, len(metadata.Split.ChildLeadersAtBootstrap))
				for shardID, leader := range metadata.Split.ChildLeadersAtBootstrap {
					cloned[shardID] = leader
				}
				return cloned
			}(),
		}
		if metadata.Split.ParentShardId != 0 {
			record.Split.ParentShardId = valuePtr(metadata.Split.ParentShardId)
		}
	}
	return record
}

func shardStatusFromProto(status metadatapb.ShardStatus) model.ShardStatus {
	switch status {
	case metadatapb.ShardStatus_SHARD_STATUS_STEADY:
		return model.ShardStatusSteadyState
	case metadatapb.ShardStatus_SHARD_STATUS_ELECTION:
		return model.ShardStatusElection
	case metadatapb.ShardStatus_SHARD_STATUS_DELETING:
		return model.ShardStatusDeleting
	default:
		return model.ShardStatusUnknown
	}
}

func shardStatusToProto(status model.ShardStatus) metadatapb.ShardStatus {
	switch status {
	case model.ShardStatusSteadyState:
		return metadatapb.ShardStatus_SHARD_STATUS_STEADY
	case model.ShardStatusElection:
		return metadatapb.ShardStatus_SHARD_STATUS_ELECTION
	case model.ShardStatusDeleting:
		return metadatapb.ShardStatus_SHARD_STATUS_DELETING
	default:
		return metadatapb.ShardStatus_SHARD_STATUS_UNKNOWN
	}
}

func splitPhaseFromProto(phase metadatapb.SplitPhase) model.SplitPhase {
	switch phase {
	case metadatapb.SplitPhase_SPLIT_PHASE_CATCH_UP:
		return model.SplitPhaseCatchUp
	case metadatapb.SplitPhase_SPLIT_PHASE_CUTOVER:
		return model.SplitPhaseCutover
	default:
		return model.SplitPhaseBootstrap
	}
}

func splitPhaseToProto(phase model.SplitPhase) metadatapb.SplitPhase {
	switch phase {
	case model.SplitPhaseCatchUp:
		return metadatapb.SplitPhase_SPLIT_PHASE_CATCH_UP
	case model.SplitPhaseCutover:
		return metadatapb.SplitPhase_SPLIT_PHASE_CUTOVER
	default:
		return metadatapb.SplitPhase_SPLIT_PHASE_BOOTSTRAP
	}
}

func serverPtrFromIdentifier(id string, resolve func(string) (model.Server, bool)) *model.Server {
	if id == "" {
		return nil
	}
	server, ok := resolve(id)
	if !ok {
		server = model.Server{Public: id, Internal: id}
	}
	return &server
}

func serversFromIdentifiers(ids []string, resolve func(string) (model.Server, bool)) []model.Server {
	if len(ids) == 0 {
		return nil
	}
	servers := make([]model.Server, 0, len(ids))
	for _, id := range ids {
		server, ok := resolve(id)
		if !ok {
			server = model.Server{Public: id, Internal: id}
		}
		servers = append(servers, server)
	}
	return servers
}

func serverIdentifier(server *model.Server) string {
	if server == nil {
		return ""
	}
	return server.GetIdentifier()
}

func serverIdentifiers(servers []model.Server) []string {
	if len(servers) == 0 {
		return nil
	}
	identifiers := make([]string, 0, len(servers))
	for _, server := range servers {
		identifiers = append(identifiers, server.GetIdentifier())
	}
	return identifiers
}

func valuePtr[T any](value T) *T {
	return &value
}
