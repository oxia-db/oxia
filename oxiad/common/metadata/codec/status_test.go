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

package codec

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
)

func TestStatusCodecMarshalYAMLWhileSharedSubMessageIsFormatted(t *testing.T) {
	server := &commonproto.DataServerIdentity{Public: "localhost:6648", Internal: "localhost:6649"}
	status := &commonproto.ClusterStatus{
		Namespaces: map[string]*commonproto.NamespaceStatus{
			"default": {
				ReplicationFactor: 1,
				Shards: map[int64]*commonproto.ShardMetadata{
					0: {Ensemble: []*commonproto.DataServerIdentity{server}},
				},
			},
		},
	}

	// Formatting a message stores its reflection state the first time, as a
	// log of the server does while the coordinator persists the status.
	formatted := make(chan struct{})
	go func() {
		defer close(formatted)
		_ = server.String()
	}()
	data, err := ClusterStatusCodec.MarshalYAML(status)
	<-formatted

	require.NoError(t, err)
	require.Contains(t, string(data), "public: localhost:6648")
}

func TestStatusCodecUnmarshalYAMLRejectsInvalidYAML(t *testing.T) {
	status, err := ClusterStatusCodec.UnmarshalYAML([]byte("namespaces: [unterminated"))
	require.Error(t, err)
	require.Nil(t, status)
}

func TestStatusCodecUnmarshalJSONRejectsInvalidJSON(t *testing.T) {
	status, err := ClusterStatusCodec.UnmarshalJSON([]byte(`{"namespaces": "not a map"}`))
	require.Error(t, err)
	require.Nil(t, status)
}
