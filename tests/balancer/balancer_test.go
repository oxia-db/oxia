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

package balancer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	coordreconciler "github.com/oxia-db/oxia/oxiad/coordinator/reconciler"
	coordrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
	"github.com/oxia-db/oxia/tests/mock"
)

func TestBalancer(t *testing.T) {
	const (
		balanceTimeout = 60 * time.Second
		balanceTick    = 50 * time.Millisecond
	)

	options := coordoption.NewDefaultOptions()
	options.Metadata.ProviderName = metadatacommon.NameMemory

	metadataFactory, err := coordmetadata.New(t.Context(), options)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, metadataFactory.Close())
	})

	metadata, err := metadataFactory.CreateMetadata(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
	})
	metadata.GetConfig().UnsafeBorrow().LoadBalancer = &proto.LoadBalancer{
		ScheduleInterval: "10ms",
		QuarantineTime:   "1ms",
	}

	coordinatorInstance, err := coordruntime.New(metadata, coordrpc.NewRpcProviderFactory(nil))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, coordinatorInstance.Close())
	})

	reconciler := coordreconciler.New(t.Context(), coordinatorInstance)
	t.Cleanup(func() {
		require.NoError(t, reconciler.Close())
	})

	waitForBalanced := func() {
		t.Helper()

		coordinatorInstance.LoadBalancer().Trigger()
		require.Eventually(t, func() bool {
			return coordinatorInstance.LoadBalancer().IsBalanced()
		}, balanceTimeout, balanceTick)
	}

	s1, s1Addr := mock.NewServer(t, "sv-1")
	defer s1.Close()
	s2, s2Addr := mock.NewServer(t, "sv-2")
	defer s2.Close()
	s3, s3Addr := mock.NewServer(t, "sv-3")
	defer s3.Close()

	require.NoError(t, metadata.CreateDataServer(&proto.DataServer{Identity: s1Addr}))
	require.NoError(t, metadata.CreateDataServer(&proto.DataServer{Identity: s2Addr}))
	require.NoError(t, metadata.CreateDataServer(&proto.DataServer{Identity: s3Addr}))
	require.NoError(t, metadata.CreateNamespace(&proto.Namespace{
		Name:              "ns-1",
		InitialShardCount: 5,
		ReplicationFactor: 3,
		KeySorting:        "natural",
	}))
	waitForBalanced()

	s4, s4Addr := mock.NewServer(t, "sv-4")
	defer s4.Close()
	require.NoError(t, metadata.CreateDataServer(&proto.DataServer{Identity: s4Addr}))

	s5, s5Addr := mock.NewServer(t, "sv-5")
	defer s5.Close()
	require.NoError(t, metadata.CreateDataServer(&proto.DataServer{Identity: s5Addr}))
	waitForBalanced()

	require.NoError(t, metadata.CreateNamespace(&proto.Namespace{
		Name:              "ns-2",
		InitialShardCount: 5,
		ReplicationFactor: 3,
		KeySorting:        "natural",
	}))
	waitForBalanced()
}
