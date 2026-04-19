package metadata

import (
	"context"
	"errors"
	"fmt"

	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	metadata_v1 "github.com/oxia-db/oxia/oxiad/coordinator/metadata/v1"
	v2backend "github.com/oxia-db/oxia/oxiad/coordinator/metadata/v2/backend"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/policy"
)

var ErrV2MetadataAlreadyInitialized = errors.New("metadata v2 already initialized")

type V1ToV2MigrationOptions struct {
	AcquireSourceLeadership func(context.Context) error
}

func MigrateV1ToV2(
	ctx context.Context,
	source metadata_v1.Provider,
	destination v2backend.Backend,
	clusterConfig *model.ClusterConfig,
	options V1ToV2MigrationOptions,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if source == nil {
		return errors.New("source metadata provider is nil")
	}
	if destination == nil {
		return errors.New("destination metadata backend is nil")
	}

	if err := acquireSourceLeadership(ctx, source, options); err != nil {
		return err
	}
	if err := waitForLeaseHeld(ctx, destination.LeaseWatch()); err != nil {
		return err
	}

	clusterStatus, version, err := source.Get()
	if err != nil {
		return err
	}
	if clusterStatus == nil || version == metadata_v1.NotExists {
		return metadata_v1.ErrMetadataNotInitialized
	}

	configRecord, statusRecord, err := ConvertV1ToV2(clusterConfig, clusterStatus)
	if err != nil {
		return err
	}

	currentConfig := destination.Load(v2backend.ConfigRecordName)
	if !isEmptyDestinationRecord(v2backend.ConfigRecordName, currentConfig) {
		return fmt.Errorf("%w: %s", ErrV2MetadataAlreadyInitialized, v2backend.ConfigRecordName)
	}
	currentStatus := destination.Load(v2backend.StatusRecordName)
	if !isEmptyDestinationRecord(v2backend.StatusRecordName, currentStatus) {
		return fmt.Errorf("%w: %s", ErrV2MetadataAlreadyInitialized, v2backend.StatusRecordName)
	}

	if err := destination.Store(v2backend.ConfigRecordName, &v2backend.Versioned[gproto.Message]{
		Version: currentVersion(currentConfig),
		Value:   configRecord,
	}); err != nil {
		return err
	}
	if err := destination.Store(v2backend.StatusRecordName, &v2backend.Versioned[gproto.Message]{
		Version: currentVersion(currentStatus),
		Value:   statusRecord,
	}); err != nil {
		return err
	}
	return nil
}

func ConvertV1ToV2(
	clusterConfig *model.ClusterConfig,
	clusterStatus *model.ClusterStatus,
) (*metadatapb.Cluster, *metadatapb.ClusterState, error) {
	if clusterConfig == nil {
		return nil, nil, errors.New("cluster config is nil")
	}
	if clusterStatus == nil {
		return nil, nil, errors.New("cluster status is nil")
	}

	configRecord := &metadatapb.Cluster{
		DataServers:             make(map[string]*metadatapb.DataServer, len(clusterConfig.Servers)),
		Namespaces:              make(map[string]*metadatapb.Namespace, len(clusterConfig.Namespaces)),
		AllowedExtraAuthorities: append([]string(nil), clusterConfig.AllowExtraAuthorities...),
	}
	if clusterConfig.LoadBalancer != nil {
		configRecord.LoadBalancer = &metadatapb.LoadBalancerPolicies{
			ScheduleInterval: durationpb.New(clusterConfig.LoadBalancer.ScheduleInterval),
			QuarantineTime:   durationpb.New(clusterConfig.LoadBalancer.QuarantineTime),
		}
	}

	for _, server := range clusterConfig.Servers {
		name := server.GetIdentifier()
		configRecord.DataServers[name] = &metadatapb.DataServer{
			Name:            name,
			PublicAddress:   server.Public,
			InternalAddress: server.Internal,
			Labels:          cloneLabels(clusterConfig.ServerMetadata[name].Labels),
		}
	}

	for _, namespace := range clusterConfig.Namespaces {
		configRecord.Namespaces[namespace.Name] = &metadatapb.Namespace{
			Name:     namespace.Name,
			Policies: convertNamespacePolicies(namespace),
		}
	}

	statusRecord := &metadatapb.ClusterState{
		Namespaces:       make(map[string]*metadatapb.NamespaceState, len(clusterStatus.Namespaces)),
		ShardIdGenerator: clusterStatus.ShardIdGenerator,
	}

	for namespaceName, namespaceStatus := range clusterStatus.Namespaces {
		shards := make(map[int64]*metadatapb.ShardState, len(namespaceStatus.Shards))
		for shardID, shard := range namespaceStatus.Shards {
			shards[shardID] = convertShardMetadata(shard)
		}
		statusRecord.Namespaces[namespaceName] = &metadatapb.NamespaceState{
			ReplicationFactor: namespaceStatus.ReplicationFactor,
			Shards:            shards,
		}
	}

	return configRecord, statusRecord, nil
}

func acquireSourceLeadership(ctx context.Context, source metadata_v1.Provider, options V1ToV2MigrationOptions) error {
	if options.AcquireSourceLeadership != nil {
		return options.AcquireSourceLeadership(ctx)
	}

	done := make(chan error, 1)
	go func() {
		done <- source.WaitToBecomeLeader()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func waitForLeaseHeld(ctx context.Context, watch *commonoption.Watch[metadatapb.LeaseState]) error {
	if watch == nil {
		return errors.New("destination lease watch is nil")
	}

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

func isEmptyDestinationRecord(
	name v2backend.MetaRecordName,
	record *v2backend.Versioned[gproto.Message],
) bool {
	if record == nil {
		return true
	}
	if record.Version != "" {
		return false
	}

	switch name {
	case v2backend.ConfigRecordName:
		msg, ok := record.Value.(*metadatapb.Cluster)
		return ok && gproto.Equal(msg, &metadatapb.Cluster{})
	case v2backend.StatusRecordName:
		msg, ok := record.Value.(*metadatapb.ClusterState)
		return ok && gproto.Equal(msg, &metadatapb.ClusterState{})
	default:
		return false
	}
}

func currentVersion(record *v2backend.Versioned[gproto.Message]) string {
	if record == nil {
		return ""
	}
	return record.Version
}

func convertNamespacePolicies(namespace model.NamespaceConfig) *metadatapb.HierarchyPolicies {
	policiesRecord := &metadatapb.HierarchyPolicies{
		InitShardCount:      valuePtr(namespace.InitialShardCount),
		ReplicationFactor:   valuePtr(namespace.ReplicationFactor),
		NotificationEnabled: valuePtr(namespace.NotificationsEnabled.Get()),
	}
	if namespace.KeySorting != "" {
		policiesRecord.KeySort = valuePtr(string(namespace.KeySorting))
	}
	if namespace.Policies != nil {
		policiesRecord.AntiAffinities = make([]*metadatapb.AntiAffinity, 0, len(namespace.Policies.AntiAffinities))
		for _, antiAffinity := range namespace.Policies.AntiAffinities {
			policiesRecord.AntiAffinities = append(policiesRecord.AntiAffinities, convertAntiAffinity(antiAffinity))
		}
	}
	return policiesRecord
}

func convertAntiAffinity(antiAffinity policy.AntiAffinity) *metadatapb.AntiAffinity {
	mode := metadatapb.AntiAffinityMode_ANTI_AFFINITY_MODE_STRICT
	if antiAffinity.Mode == policy.Relaxed {
		mode = metadatapb.AntiAffinityMode_ANTI_AFFINITY_MODE_RELAX
	}
	return &metadatapb.AntiAffinity{
		Labels: append([]string(nil), antiAffinity.Labels...),
		Mode:   mode,
	}
}

func convertShardMetadata(shard model.ShardMetadata) *metadatapb.ShardState {
	record := &metadatapb.ShardState{
		Status:                  convertShardStatus(shard.Status),
		Term:                    shard.Term,
		Leader:                  serverIdentifier(shard.Leader),
		Ensemble:                serverIdentifiers(shard.Ensemble),
		RemovedNodes:            serverIdentifiers(shard.RemovedNodes),
		PendingDeleteShardNodes: serverIdentifiers(shard.PendingDeleteShardNodes),
		Int32HashRange: &metadatapb.Int32HashRange{
			Min: shard.Int32HashRange.Min,
			Max: shard.Int32HashRange.Max,
		},
	}
	if shard.Split != nil {
		record.Split = &metadatapb.ShardSplittingState{
			Phase:                   convertSplitPhase(shard.Split.Phase),
			ParentShardId:           valuePtr(shard.Split.ParentShardId),
			ChildShardIds:           append([]int64(nil), shard.Split.ChildShardIDs...),
			SplitPoint:              shard.Split.SplitPoint,
			SnapshotOffset:          valuePtr(shard.Split.SnapshotOffset),
			ParentTermAtBootstrap:   valuePtr(shard.Split.ParentTermAtBootstrap),
			ChildLeadersAtBootstrap: cloneChildLeaders(shard.Split.ChildLeadersAtBootstrap),
		}
	}
	return record
}

func convertShardStatus(status model.ShardStatus) metadatapb.ShardStatus {
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

func convertSplitPhase(phase model.SplitPhase) metadatapb.SplitPhase {
	switch phase {
	case model.SplitPhaseCatchUp:
		return metadatapb.SplitPhase_SPLIT_PHASE_CATCH_UP
	case model.SplitPhaseCutover:
		return metadatapb.SplitPhase_SPLIT_PHASE_CUTOVER
	default:
		return metadatapb.SplitPhase_SPLIT_PHASE_BOOTSTRAP
	}
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

func cloneChildLeaders(leaders map[int64]string) map[int64]string {
	if len(leaders) == 0 {
		return nil
	}
	cloned := make(map[int64]string, len(leaders))
	for shardID, leader := range leaders {
		cloned[shardID] = leader
	}
	return cloned
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
