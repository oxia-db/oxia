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

// Package model holds the data that the scheduler, the planner and the
// dispatcher exchange: plain values that can be logged, serialized and
// replayed.
package model

import (
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
)

// Snapshot is the view of the cluster that one round plans on. It is built
// for the round and nobody modifies it after; it may borrow its labels and
// identities from the metadata.
type Snapshot struct {
	// DataServers holds every data server of the configuration, and every removed
	// one that is still a member of an ensemble, by name: every member of an
	// ensemble is in it.
	DataServers map[string]*DataServer
	// Namespaces holds the shards of each namespace, by name.
	Namespaces map[string]*Namespace
}

type DataServer struct {
	Identity *proto.DataServerIdentity
	Labels   map[string]string
	// Removed is set for a data server that is no longer in the configuration.
	// Its replicas are drained.
	Removed bool
	// Running is set for a data server that answers its health checks.
	Running bool
	// Stable is set for a data server that has been running long enough to
	// take a replica: moving replicas onto a data server that flaps turns
	// one failure into a loop of moves.
	Stable bool
}

type Namespace struct {
	AntiAffinities []*proto.AntiAffinity
	// Shards holds the shards by ID, which is unique in the cluster.
	Shards map[int64]*Shard
}

type Shard struct {
	// Ensemble holds the names of the members.
	Ensemble []string
	Leader   string
	// Frozen is set for a shard that no op may change now: part of a split,
	// with a pending change of its ensemble, or with an op in flight or
	// backing off.
	Frozen bool
	// Changing is set while a change of its ensemble is pending: a replica
	// is still on its way.
	Changing bool
}

// ConfiguredDataServers returns the number of data servers in the
// configuration: the ones that aren't removed.
func (s *Snapshot) ConfiguredDataServers() int {
	count := 0
	for _, dataServer := range s.DataServers {
		if !dataServer.Removed {
			count++
		}
	}
	return count
}

// Shard returns the namespace and the shard that op changes, or nil if the
// snapshot doesn't have them.
func (s *Snapshot) Shard(op Op) (*Namespace, *Shard) {
	ns, ok := s.Namespaces[op.Namespace]
	if !ok {
		return nil, nil
	}
	return ns, ns.Shards[op.Shard]
}

// NewSnapshot builds the snapshot of the data servers and the statuses of the
// namespaces, with the anti-affinities of their configuration. Every data
// server is down until its owner sets its health. A shard is frozen while it is
// part of a split or a change of its ensemble is pending. The shards being
// deleted are left out: their replicas are going away.
func NewSnapshot(dataServers map[string]commonobject.Borrowed[*proto.DataServer],
	namespaces map[string]commonobject.Borrowed[*proto.Namespace],
	statuses map[string]commonobject.Borrowed[*proto.NamespaceStatus]) *Snapshot {
	s := &Snapshot{
		DataServers: make(map[string]*DataServer, len(dataServers)),
		Namespaces:  make(map[string]*Namespace, len(statuses)),
	}
	for name, borrowed := range dataServers {
		dataServer := borrowed.UnsafeBorrow()
		s.DataServers[name] = &DataServer{Identity: dataServer.GetIdentity(), Labels: dataServer.GetMetadata().GetLabels()}
	}
	for name, borrowed := range statuses {
		ns := &Namespace{Shards: make(map[int64]*Shard)}
		if config, ok := namespaces[name]; ok {
			ns.AntiAffinities = config.UnsafeBorrow().GetAntiAffinities()
		}
		for id, shard := range borrowed.UnsafeBorrow().GetShards() {
			if shard.GetStatusOrDefault() == proto.ShardStatusDeleting {
				continue
			}
			ensemble := make([]string, 0, len(shard.GetEnsemble()))
			for _, member := range shard.GetEnsemble() {
				memberName := member.GetNameOrDefault()
				if _, ok := s.DataServers[memberName]; !ok {
					s.DataServers[memberName] = &DataServer{Identity: member, Removed: true}
				}
				ensemble = append(ensemble, memberName)
			}
			ns.Shards[id] = &Shard{
				Ensemble: ensemble,
				Leader:   shard.GetLeader().GetNameOrDefault(),
				Frozen:   shard.GetSplit() != nil || shard.GetEnsembleChange() != nil,
				Changing: shard.GetEnsembleChange() != nil,
			}
		}
		s.Namespaces[name] = ns
	}
	return s
}
