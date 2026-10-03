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

package runtime

import (
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector/ensemble"
	shardcontroller "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/shard"
)

type failingNamespaceMetadata struct {
	coordmetadata.Metadata
	err      error
	proposed *proto.NamespaceStatus
}

func (m *failingNamespaceMetadata) CreateNamespaceStatus(_ string, status *proto.NamespaceStatus) error {
	m.proposed = status
	return m.err
}

type namespaceEnsembleSelector struct{}

func (namespaceEnsembleSelector) Select(ctx *ensemble.Context) ([]string, error) {
	return ctx.Candidates.Values(), nil
}

func TestCreateNamespaceHandlesStatusErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"already exists", fmt.Errorf("namespace creation: %w", metadatacommon.ErrAlreadyExists)},
		{"write failed", errors.New("status store unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
			namespace := &proto.Namespace{Name: "default", InitialShardCount: 1, ReplicationFactor: 1}
			metadata := &failingNamespaceMetadata{
				Metadata: newTestMetadata(t, &proto.ClusterConfiguration{
					Servers:    []*proto.DataServerIdentity{server},
					Namespaces: []*proto.Namespace{namespace},
				}),
				err: tc.err,
			}
			c := &runtime{
				ctx:              t.Context(),
				logger:           slog.Default(),
				metadata:         metadata,
				ensembleSelector: namespaceEnsembleSelector{},
				shardControllers: make(map[int64]shardcontroller.Controller),
			}

			require.ErrorIs(t, c.CreateNamespace(namespace.Name, namespace), tc.err)
			require.NotNil(t, metadata.proposed)
			require.Len(t, metadata.proposed.Shards, 1)
			require.Empty(t, c.shardControllers)
			_, exists := metadata.GetNamespaceStatus(namespace.Name)
			require.False(t, exists)
		})
	}
}
