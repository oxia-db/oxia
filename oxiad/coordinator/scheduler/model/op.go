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

package model

// OpKind is what an op changes.
type OpKind int

const (
	// Replace replaces the member From of the ensemble of a shard with To,
	// which copies a whole replica onto To and runs an election.
	Replace OpKind = iota
	// TransferLeader moves the leadership of a shard from its leader From to
	// the member To, with an election that prefers To.
	TransferLeader
)

func (k OpKind) String() string {
	if k == TransferLeader {
		return "transfer-leader"
	}
	return "replace"
}

// Op is a change of a shard that the scheduler makes.
type Op struct {
	Kind      OpKind
	Namespace string
	Shard     int64
	From      string
	To        string
	// Reason says which objective the op improves, for the decision log.
	Reason string
}
