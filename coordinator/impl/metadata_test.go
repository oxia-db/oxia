// Copyright 2023 StreamNative, Inc.
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

package impl

import (
	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/kubernetes/fake"
	"oxia/coordinator/model"
	k8sTesting "oxia/kubernetes/testing"
	"path/filepath"
	"testing"
)

var (
	_fake = func() *fake.Clientset {
		f := fake.NewSimpleClientset()
		f.PrependReactor("*", "*", k8sTesting.ResourceVersionSupport(f.Tracker()))
		return f
	}()
	metadataProviders = map[string]func(t *testing.T) MetadataProvider{
		"memory": func(t *testing.T) MetadataProvider {
			return NewMetadataProviderMemory()
		},
		"file": func(t *testing.T) MetadataProvider {
			return NewMetadataProviderFile(filepath.Join(t.TempDir(), "metadata"))
		},
		"configmap": func(t *testing.T) MetadataProvider {
			return NewMetadataProviderConfigMap(_fake, "ns", "n")
		},
	}
)

func TestMetadataProvider(t *testing.T) {
	for name, provider := range metadataProviders {
		t.Run(name, func(t *testing.T) {
			m := provider(t)

			res, version, err := m.Get()
			assert.NoError(t, err)
			assert.Equal(t, MetadataNotExists, version)
			assert.Nil(t, res)

			newVersion, err := m.Store(&model.ClusterStatus{
				ReplicationFactor: 3,
				Shards:            make(map[uint32]model.ShardMetadata),
			}, "")
			assert.ErrorIs(t, err, ErrorMetadataBadVersion)
			assert.Equal(t, MetadataNotExists, newVersion)

			newVersion, err = m.Store(&model.ClusterStatus{
				ReplicationFactor: 3,
				Shards:            make(map[uint32]model.ShardMetadata),
			}, MetadataNotExists)
			assert.NoError(t, err)
			assert.EqualValues(t, Version("0"), newVersion)

			res, version, err = m.Get()
			assert.NoError(t, err)
			assert.EqualValues(t, Version("0"), version)
			assert.Equal(t, &model.ClusterStatus{
				ReplicationFactor: 3,
				Shards:            make(map[uint32]model.ShardMetadata),
			}, res)

			assert.NoError(t, m.Close())
		})
	}
}
