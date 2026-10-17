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

package metadata

import (
	"io"

	commonobject "github.com/oxia-db/oxia/common/object"
	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

// Metadata is the coordinator's view of the cluster metadata. It holds two
// documents, each stored by a provider (memory, file, Kubernetes or raft):
//
//   - The status, which the coordinator maintains: the cluster instance ID,
//     the shard ID generator, and for each namespace its shards, with their
//     term, leader, ensemble and split state.
//   - The configuration, which the operators maintain, through the management
//     API or the configuration file: the coordinators, the namespaces, the
//     data servers to run them on, and the load balancer settings.
//
// Reads are served from a cache of each document. The cache loads the
// document when it is empty: at first, and after a failed write, which may
// still have been applied. A read then returns the error of that load.
//
// Writes apply a change to the current document, and store the result in a
// single write: concurrent writes are serialized, and none reverts another.
// A write fails if another coordinator wrote the document since it was
// loaded, e.g. after a change of leadership.
//
// Reads and writes return their error without retrying: the caller decides
// whether and how to retry, e.g. with a backoff, or on its next round.
//
// Metadata owns the documents. The reads lend parts of them without copying,
// as commonobject.Borrowed values: a borrowed value is shared with every other
// reader, so the borrower only reads it, and only while it uses it. A write
// replaces the document instead of modifying it, so a borrowed value stays
// consistent, but no longer current: a borrower that needs to keep a value,
// or to modify it, clones it. The values returned as plain pointers, e.g. by
// GetSelf or PatchNamespace, are copies that the caller owns, and so are the
// values that the caller passes to a write: Metadata stores its own copy. The
// status that PatchNamespaceStatus passes to modify is a copy owned by the
// write, which modify changes in place but doesn't keep.
type Metadata interface {
	io.Closer

	// WaitToBecomeLeader blocks until this coordinator holds the leadership.
	// lost is closed when the leadership is lost, or nil if it can't be. It
	// fails if there is no configuration while the status has namespaces: the
	// configuration was deleted, and reconciling it as empty would delete
	// every namespace.
	WaitToBecomeLeader() (lost <-chan struct{}, err error)
	// GetSelf returns this coordinator, from the configuration.
	GetSelf() (*commonproto.Coordinator, error)
	// GetLeader returns the coordinator that holds the leadership.
	GetLeader() (*commonproto.Coordinator, error)
	// GetInstanceID returns the ID of the cluster.
	GetInstanceID() (string, error)
	// AllocateShardIDs reserves count shard IDs, and returns the first one.
	AllocateShardIDs(count uint32) (int64, error)

	// CreateNamespaceStatus creates the status of a namespace, or returns
	// ErrAlreadyExists.
	CreateNamespaceStatus(namespace string, status *commonproto.NamespaceStatus) error
	// ListNamespaceStatus returns the status of every namespace, by name.
	ListNamespaceStatus() (map[string]commonobject.Borrowed[*commonproto.NamespaceStatus], error)
	// GetNamespaceStatus returns the status of a namespace, and false if it
	// doesn't exist.
	GetNamespaceStatus(namespace string) (commonobject.Borrowed[*commonproto.NamespaceStatus], bool, error)
	// PatchNamespaceStatus applies modify to a copy of the current status of a
	// namespace, and stores it in a single write, unless modify returns false.
	// A namespace left without shards is deleted. It returns ErrNotFound if
	// the namespace doesn't exist.
	PatchNamespaceStatus(namespace string, modify func(status *commonproto.NamespaceStatus) bool) error
	// DeleteNamespaceStatus marks the shards of a namespace as deleting, and
	// returns its status. Unlike the other writes, it retries until the write
	// succeeds or the metadata closes, and returns an empty value then.
	DeleteNamespaceStatus(namespace string) commonobject.Borrowed[*commonproto.NamespaceStatus]

	// GetShardStatus returns the metadata of a shard, and false if it doesn't
	// exist.
	GetShardStatus(namespace string, shard int64) (commonobject.Borrowed[*commonproto.ShardMetadata], bool, error)
	// UpdateShardStatus replaces the metadata of an existing shard, or returns
	// ErrNotFound.
	UpdateShardStatus(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata) error
	// DeleteShardStatus deletes a shard, and its namespace if it was the last
	// shard.
	DeleteShardStatus(namespace string, shard int64) error

	// GetConfig returns the configuration.
	GetConfig() (commonobject.Borrowed[*commonproto.ClusterConfiguration], error)
	// SubscribeConfig returns a subscription notified on each change of the
	// configuration.
	SubscribeConfig() *cache.Subscription[provider.Versioned[*commonproto.ClusterConfiguration]]
	// GetLoadBalancerConfig returns the load balancer configuration, with its
	// defaults.
	GetLoadBalancerConfig() (commonobject.Borrowed[*commonproto.LoadBalancer], error)

	// CreateNamespace adds a namespace to the configuration, or returns
	// ErrAlreadyExists, or ErrFailedPrecondition if there are fewer data
	// servers than its replication factor.
	CreateNamespace(namespace *commonproto.Namespace) error
	// PatchNamespace updates the replication factor and the notifications of
	// a namespace, and returns it, or ErrNotFound.
	PatchNamespace(namespace *commonproto.Namespace) (*commonproto.Namespace, error)
	// DeleteNamespace removes a namespace from the configuration, and returns
	// it, or ErrNotFound.
	DeleteNamespace(name string) (*commonproto.Namespace, error)
	// ListNamespace returns the configuration of every namespace, by name.
	ListNamespace() (map[string]commonobject.Borrowed[*commonproto.Namespace], error)
	// GetNamespace returns the configuration of a namespace, and false if it
	// doesn't exist.
	GetNamespace(name string) (commonobject.Borrowed[*commonproto.Namespace], bool, error)
	// GetNamespaceTermOptions returns the NewTerm options of the shards of a
	// namespace, or the defaults if it doesn't exist.
	GetNamespaceTermOptions(name string) (*commonproto.NewTermOptions, error)

	// CreateDataServer adds a data server to the configuration, or returns
	// ErrAlreadyExists.
	CreateDataServer(dataServer *commonproto.DataServer) error
	// PatchDataServer updates the addresses and the labels of a data server,
	// and returns it, or ErrNotFound.
	PatchDataServer(dataServer *commonproto.DataServer) (*commonproto.DataServer, error)
	// DeleteDataServer removes a data server from the configuration, and
	// returns it, or ErrNotFound, or ErrFailedPrecondition if a namespace
	// would be left with fewer data servers than its replication factor.
	DeleteDataServer(name string) (*commonproto.DataServer, error)
	// ListDataServer returns every data server, with its identity and its
	// metadata, by name: the internal address of a data server without a
	// name.
	ListDataServer() (map[string]commonobject.Borrowed[*commonproto.DataServer], error)
	// GetDataServer returns a data server, with its identity and its
	// metadata, and false if it doesn't exist.
	GetDataServer(name string) (commonobject.Borrowed[*commonproto.DataServer], bool, error)
}
