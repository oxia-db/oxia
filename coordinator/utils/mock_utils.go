// Copyright 2023-2025 The Oxia Authors
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

package utils

import "github.com/oxia-db/oxia/coordinator/model"

func SimpleEnsembleSupplier(candidates []model.Server, nc *model.NamespaceConfig, cs *model.ClusterStatus) []model.Server {
	n := len(candidates)
	res := make([]model.Server, nc.ReplicationFactor)
	for i := uint32(0); i < nc.ReplicationFactor; i++ {
		res[i] = candidates[int(cs.ServerIdx+i)%n]
	}
	return res
}
