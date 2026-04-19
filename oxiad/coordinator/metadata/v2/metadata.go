package metadata

import (
	"io"

	"github.com/oxia-db/oxia/common/proto/metadata"
	"github.com/oxia-db/oxia/oxiad/common/option"
)

type Store interface {
	io.Closer

	LeaseWatch() *option.Watch[metadata.LeaseState]

	GetAllowedAuthorities() []string
	DeleteExtraAllowedAuthorities(authorities []string) error
	AddExtraAllowedAuthorities(authorities []string) error
	PatchAllowedAuthorities(authorities []string) error

	GetLoadBalancerPolicies() *metadata.LoadBalancerPolicies
	PatchLoadBalancerPolicies(policies *metadata.LoadBalancerPolicies) (*metadata.LoadBalancerPolicies, error)

	GetClusterHierarchyPolicies() *metadata.HierarchyPolicies
	PatchClusterHierarchyPolicies(policies *metadata.HierarchyPolicies) (*metadata.HierarchyPolicies, error)

	GetDataServer(name string) (*metadata.DataServer, error)
	DeleteDataServers(names []string) error
	CreateDataServers(dataServers []*metadata.DataServer) error
	ListDataServer() ([]*metadata.DataServer, error)
	PatchDataServer(dataServer *metadata.DataServer) (*metadata.DataServer, error)

	GetNamespace(name string) (*metadata.Namespace, error)
	DeleteNamespaces(names []string) error
	CreateNamespaces(namespaces []*metadata.Namespace) error
	ListNamespace() ([]*metadata.Namespace, error)
	PatchNamespace(namespace *metadata.Namespace) (*metadata.Namespace, error)

	GetNamespaceHierarchyPolicies(name string) *metadata.HierarchyPolicies
	PatchNamespaceHierarchyPolicies(name string, policy *metadata.HierarchyPolicies) (*metadata.HierarchyPolicies, error)

	GetNamespaceState(name string) (*metadata.NamespaceState, error)
	DeleteNamespaceStates(names []string) error
	CreateNamespaceStates(namespace map[string]*metadata.NamespaceState) error
	PatchNamespaceState(name string, namespace *metadata.NamespaceState) (*metadata.NamespaceState, error)

	GetShardState(namespace string, shardID int64) (*metadata.ShardState, error)
	PatchShardState(namespace string, shardID int64, shard *metadata.ShardState) (*metadata.ShardState, error)
}
