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

type listOptions struct {
	baseOptions

	secondaryIndexName *string
}

// ListOption represents an option for the [SyncClient.List] operation.
type ListOption interface {
	applyList(opts *listOptions)
	applyRangeScan(opts *rangeScanOptions)
	applyGet(opts *getOptions)
}

func newListOptions(opts []ListOption) *listOptions {
	listOpts := &listOptions{}
	for _, opt := range opts {
		opt.applyList(listOpts)
	}
	return listOpts
}

type useIndex struct {
	indexName string
}

func (u *useIndex) applyList(opts *listOptions) {
	opts.secondaryIndexName = &u.indexName
}

func (u *useIndex) applyRangeScan(opts *rangeScanOptions) {
	opts.secondaryIndexName = &u.indexName
}

func (u *useIndex) applyGet(opts *getOptions) {
	opts.secondaryIndexName = &u.indexName
}

// UseIndex let the users specify a different index to follow for the
// Note: The returned list will contain they primary keys of the records.
func UseIndex(indexName string) ListOption {
	return &useIndex{indexName}
}
