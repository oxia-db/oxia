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

package kubernetes

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	metadataprovider "github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

func TestProviderUsesConfiguredCoordinatorName(t *testing.T) {
	p, err := NewProvider(
		t.Context(),
		fake.NewSimpleClientset(),
		"ns",
		"status",
		metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled,
		"coordinator-0",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	provider := p.(*Provider[*commonproto.ClusterStatus])
	require.Equal(t, "coordinator-0", provider.coordinatorName)
}

func TestProviderRejectsEmptyCoordinatorName(t *testing.T) {
	_, err := NewProvider(
		t.Context(),
		fake.NewSimpleClientset(),
		"ns",
		"status",
		metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled,
		" ",
	)
	require.ErrorContains(t, err, "coordinator name must not be empty")
}

func TestProviderGetLeaderNameUsesLeaderElector(t *testing.T) {
	kc := fake.NewSimpleClientset()

	p, err := NewProvider(
		t.Context(),
		kc,
		"ns",
		"status",
		metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled,
		"coordinator-0",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	_, err = p.WaitToBecomeLeader()
	require.NoError(t, err)

	leader, err := p.GetLeaderName()
	require.NoError(t, err)
	require.Equal(t, "coordinator-0", leader)
}

func TestProviderGetLeaderNameUnavailable(t *testing.T) {
	p, err := NewProvider(
		t.Context(),
		fake.NewSimpleClientset(),
		"ns",
		"status",
		metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled,
		"coordinator-0",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	_, err = p.GetLeaderName()
	require.True(t, errors.Is(err, metadataprovider.ErrCoordinatorLeaderUnavailable))
}
