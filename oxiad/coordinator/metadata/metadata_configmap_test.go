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
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestMetadataProviderConfigMapReleasesLeaseOnClose(t *testing.T) {
	kc := fake.NewSimpleClientset()
	m := NewMetadataProviderConfigMap(kc, "ns", "coordinator-lease")
	require.NoError(t, m.WaitToBecomeLeader())

	hostname, err := os.Hostname()
	require.NoError(t, err)
	lease, err := kc.CoordinationV1().Leases("ns").Get(context.Background(), "coordinator-lease", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, hostname, ptr.Deref(lease.Spec.HolderIdentity, ""))

	require.NoError(t, m.Close())

	lease, err = kc.CoordinationV1().Leases("ns").Get(context.Background(), "coordinator-lease", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, ptr.Deref(lease.Spec.HolderIdentity, ""))
	assert.EqualValues(t, 1, ptr.Deref(lease.Spec.LeaseDurationSeconds, 0))
}
