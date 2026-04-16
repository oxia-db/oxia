package metadata_v2

import "github.com/oxia-db/oxia/common/proto"

type Store interface {
	GetAllowedAuthorities() []string
	DeleteExtraAllowedAuthorities(authorities []string) error
	AddExtraAllowedAuthorities(authorities []string) error
	UpdateAllowedAuthorities(authorities []string) error

	GetLoadBalancerPolicies() *proto.LoadBalancerPolicies
	PatchLoadBalancerPolicies(policies *proto.LoadBalancerPolicies) (*proto.LoadBalancerPolicies, error)

	GetClusterHierarchyPolicies(name string) *proto.HierarchyPolicies
	PatchClusterHierarchyPolicies(policies *proto.HierarchyPolicies) (*proto.HierarchyPolicies, error)

	GetDataServer(name string) (*proto.DataServer, error)
	DeleteDataServers(names []string) error
	CreateDataServers(dataServers []*proto.DataServer) error
	ListDataServer() ([]*proto.DataServer, error)
	PatchDataServer(dataServer *proto.DataServer) (proto.DataServer, error)

	GetNamespace(name string) (*proto.Namespace, error)
	DeleteNamespaces(names []string) error
	CreateNamespaces(namespaces []*proto.Namespace) error
	ListNamespace() ([]*proto.Namespace, error)
	PatchNamespace(namespace *proto.Namespace) (proto.Namespace, error)

	GetNamespaceHierarchyPolicies(name string) *proto.HierarchyPolicies
	PatchNamespaceHierarchyPolicies(policy *proto.HierarchyPolicies) (proto.HierarchyPolicies, error)

	GetNamespaceState(name string)
	DeleteNamespaceStates(names []string) error
	CreateNamespaceStates(namespace []*proto.NamespaceState) error
	PatchNamespaceState(namespace *proto.NamespaceState) (proto.NamespaceState, error)

	GetShardState(name string) (*proto.ShardState, error)
	PatchShardState(shard *proto.ShardState) (proto.ShardState, error)
}
