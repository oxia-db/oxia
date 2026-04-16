package metadata_v2

import "github.com/oxia-db/oxia/common/proto"

type Store interface {
	GetAllowedAuthorities() []string
	DeleteExtraAllowedAuthorities(authority []string) error
	AddExtraAllowedAuthorities(authority []string) error
	UpdateAllowedAuthorities(authority []string) error

	GetLoadBalancerPolicies() *proto.LoadBalancerPolicies
	PatchLoadBalancerPolicies(policy *proto.LoadBalancerPolicies) (*proto.LoadBalancerPolicies, error)

	GetClusterHierarchyPolicies(name string) *proto.HierarchyPolicies
	PatchClusterHierarchyPolicies(policy *proto.HierarchyPolicies) (*proto.HierarchyPolicies, error)

	GetDataServer(name string) (*proto.DataServer, error)
	DeleteDataServer(name string) error
	ListDataServer() ([]*proto.DataServer, error)
	CreateDataServer(dataServer *proto.DataServer) error
	PatchDataServer(dataServer *proto.DataServer) (proto.DataServer, error)

	GetNamespace(name string) (*proto.Namespace, error)
	DeleteNamespace(name string) error
	ListNamespace() ([]*proto.Namespace, error)
	CreateNamespace(namespace *proto.Namespace) error
	PatchNamespace(namespace *proto.Namespace) (proto.Namespace, error)

	GetNamespaceHierarchyPolicies(name string) *proto.HierarchyPolicies
	PatchNamespaceHierarchyPolicies(policy *proto.HierarchyPolicies) (proto.HierarchyPolicies, error)

	GetNamespaceState(name string)
	CreateNamespaceState(namespace *proto.NamespaceState) error
	PatchNamespaceState(namespace *proto.NamespaceState) (proto.NamespaceState, error)
	DeleteNamespaceState(name string) error

	GetShardState(name string) (*proto.ShardState, error)
	PatchShardState(shard *proto.ShardState) (proto.ShardState, error)
}
