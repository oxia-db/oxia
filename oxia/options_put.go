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

package oxia

import "github.com/pkg/errors"

type putOptions struct {
	baseOptions
	expectedVersion    *int64
	ephemeral          bool
	sequenceKeysDeltas []uint64
	secondaryIndexes   []*secondaryIdxOption
}

// PutOption represents an option for the [SyncClient.Put] operation.
type PutOption interface {
	applyPut(opts *putOptions)
}

func newPutOptions(opts []PutOption) (*putOptions, error) {
	putOpts := &putOptions{}
	for _, opt := range opts {
		opt.applyPut(putOpts)
	}

	if len(putOpts.sequenceKeysDeltas) > 0 {
		if putOpts.partitionKey == nil {
			return nil, errors.Wrap(ErrInvalidOptions, "usage of sequential keys requires PartitionKey() to be set")
		}

		if putOpts.expectedVersion != nil {
			return nil, errors.Wrap(ErrInvalidOptions, "usage of sequential keys does not allow to specify an ExpectedVersionId")
		}

		if putOpts.sequenceKeysDeltas[0] == 0 {
			return nil, errors.Wrap(ErrInvalidOptions, "first delta in sequence keys delta must always be > 0")
		}
	}

	return putOpts, nil
}

// ExpectedRecordNotExists Marks that the put operation should only be successful
// if the record does not exist yet.
func ExpectedRecordNotExists() PutOption {
	return &expectedVersionId{VersionIdNotExists}
}

type ephemeral struct{}

var ephemeralFlag = &ephemeral{}

func (*ephemeral) applyPut(opts *putOptions) {
	opts.ephemeral = true
}

// Ephemeral marks the record to be created as an ephemeral record.
// Ephemeral records have their lifecycle tied to a particular client instance, and they
// are automatically deleted when the client instance is closed.
// These records are also deleted if the client cannot communicate with the Oxia
// service for some extended amount of time, and the session between the client and
// the service "expires".
// Application can control the session behavior by setting the session timeout
// appropriately with [WithSessionTimeout] option when creating the client instance.
func Ephemeral() PutOption {
	return ephemeralFlag
}

type sequenceKeysDeltas struct {
	sequenceKeysDeltas []uint64
}

func (s *sequenceKeysDeltas) applyPut(opts *putOptions) {
	opts.sequenceKeysDeltas = s.sequenceKeysDeltas
}

// SequenceKeysDeltas will request that the final record key to be
// assigned by the server, based on the prefix record key and
// appending one or more sequences.
// The sequence numbers will be atomically added based on the deltas.
// Deltas must be >= 0 and the first one strictly > 0.
// SequenceKeysDeltas also requires that a [PartitionKey] option is
// provided.
func SequenceKeysDeltas(delta ...uint64) PutOption {
	return &sequenceKeysDeltas{delta}
}

type secondaryIdxOption struct {
	indexName    string
	secondaryKey string
}

func (s *secondaryIdxOption) applyPut(opts *putOptions) {
	opts.secondaryIndexes = append(opts.secondaryIndexes, s)
}

// SecondaryIndex let the users specify additional keys to index the record
// Index names are arbitrary strings and can be used in `List` and
// `RangeScan` requests.
// Secondary keys are not required to be unique.
// Multiple secondary indexes can be passed on the same record, even
// reusing multiple times the same indexName.
func SecondaryIndex(indexName string, secondaryKey string) PutOption {
	return &secondaryIdxOption{indexName, secondaryKey}
}
