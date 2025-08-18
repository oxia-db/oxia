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

package oxia

// BaseOption is an option that applies to all the client operations.
type BaseOption interface {
	PutOption
	GetOption
	DeleteOption
	DeleteRangeOption
	ListOption
	RangeScanOption
	GetSequenceUpdatesOption
}

type baseOptions struct {
	partitionKey *string
}

type baseOptionsIf interface {
	PartitionKey() *string
}

func (o *baseOptions) PartitionKey() *string {
	return o.partitionKey
}

// --------------------------------------------------------------------------------------------

type partitionKeyOpt struct {
	partitionKey *string
}

func (o *partitionKeyOpt) applyPut(opts *putOptions) {
	opts.partitionKey = o.partitionKey
}

func (o *partitionKeyOpt) applyDelete(opts *deleteOptions) {
	opts.partitionKey = o.partitionKey
}

func (o *partitionKeyOpt) applyDeleteRange(opts *deleteRangeOptions) {
	opts.partitionKey = o.partitionKey
}

func (o *partitionKeyOpt) applyGet(opts *getOptions) {
	opts.partitionKey = o.partitionKey
}

func (o *partitionKeyOpt) applyList(opts *listOptions) {
	opts.partitionKey = o.partitionKey
}

func (o *partitionKeyOpt) applyRangeScan(opts *rangeScanOptions) {
	opts.partitionKey = o.partitionKey
}

func (o *partitionKeyOpt) applyGetSequenceUpdates(opts *getSequenceUpdatesOptions) {
	opts.partitionKey = o.partitionKey
}

// PartitionKey overrides the partition routing with the specified `partitionKey` instead
// of the regular record key.
// Records with the same partitionKey will always be guaranteed to be co-located in the
// same Oxia shard.
func PartitionKey(partitionKey string) BaseOption {
	return &partitionKeyOpt{
		partitionKey: &partitionKey,
	}
}
