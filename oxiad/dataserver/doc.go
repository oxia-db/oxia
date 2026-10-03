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

// Package dataserver implements the Oxia data server: it serves the shards
// assigned to it by a coordinator and replicates data to its peers. It backs
// the `oxia server` command and can equally be embedded in a Go application,
// so that each application node hosts an Oxia data server in the same binary:
//
//	options := option.NewDefaultOptions()
//	options.Storage.Database.Dir = "./data/db"
//	options.Storage.WAL.Dir = "./data/wal"
//
//	server, err := dataserver.New(ctx, options)
//	if err != nil {
//		return err
//	}
//	defer server.Close()
//
// A data server is passive until a coordinator assigns shards to it; see the
// coordinator package for how to run a whole Oxia cluster in-process.
//
// [NewStandalone] starts a self-contained single-node server instead, with no
// replication and no coordinator: the embedded equivalent of the
// `oxia standalone` command, and the simplest option for tests and
// single-process deployments.
package dataserver
