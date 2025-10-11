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

import (
	"context"
	"errors"
	"io"

	"github.com/oxia-db/oxia/oxia/internal/batch"
)

const (
	// VersionIdNotExists represent the VersionId of a non-existing record.
	VersionIdNotExists int64 = -1
)

var (
	// ErrKeyNotFound A record associated with the specified key was not found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrUnexpectedVersionId The expected version id passed as a condition does not match
	// the current version id of the stored record.
	ErrUnexpectedVersionId = errors.New("unexpected version id")

	ErrInvalidOptions = errors.New("invalid options")

	// ErrRequestTooLarge is returned when a request is larger than the maximum batch size.
	ErrRequestTooLarge = batch.ErrRequestTooLarge

	// ErrUnknownStatus Unknown error.
	ErrUnknownStatus = errors.New("unknown status")
)

// AsyncClient Oxia client with methods suitable for asynchronous operations.
//
// This interface exposes the same functionality as [SyncClient], though it returns
// a channel instead of an actual result for the performed operations.
//
// This allows to enqueue multiple operations which the client library will be
// able to group and automatically batch.
//
// Batching of requests will ensure a larger throughput and more efficient handling.
// Applications can control the batching by configuring the linger-time with
// [WithBatchLinger] option in [NewAsyncClient].
type AsyncClient interface {
	io.Closer

	// Put Associates a value with a key
	//
	// There are few options that can be passed to the Put operation:
	//  - The Put operation can be made conditional on that the record hasn't changed from
	//    a specific existing version by passing the [ExpectedVersionId] option.
	//  - Client can assert that the record does not exist by passing [ExpectedRecordNotExists]
	//  - Client can create an ephemeral record with [Ephemeral]
	//
	// Returns a [Version] object that contains information about the newly updated record
	// Returns [ErrorUnexpectedVersionId] if the expected version id does not match the
	// current version id of the record
	Put(key string, value []byte, options ...PutOption) <-chan PutResult

	// Delete removes the key and its associated value from the data store.
	//
	// The Delete operation can be made conditional on that the record hasn't changed from
	// a specific existing version by passing the [ExpectedVersionId] option.
	// Returns [ErrorUnexpectedVersionId] if the expected version id does not match the
	// current version id of the record
	Delete(key string, options ...DeleteOption) <-chan error

	// DeleteRange deletes any records with keys within the specified range.
	// Note: Oxia uses a custom sorting order that treats `/` characters in special way.
	// Refer to this documentation for the specifics:
	// https://oxia-db.github.io/docs/features/oxia-key-sorting
	DeleteRange(minKeyInclusive string, maxKeyExclusive string, options ...DeleteRangeOption) <-chan error

	// Get returns the value associated with the specified key.
	// In addition to the value, a version object is also returned, with information
	// about the record state.
	// Returns ErrorKeyNotFound if the record does not exist
	Get(key string, options ...GetOption) <-chan GetResult

	// List any existing keys within the specified range.
	// Note: Oxia uses a custom sorting order that treats `/` characters in special way.
	// Refer to this documentation for the specifics:
	// https://oxia-db.github.io/docs/features/oxia-key-sorting
	List(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...ListOption) <-chan ListResult

	// RangeScan perform a scan for existing records with any keys within the specified range.
	// Note: Oxia uses a custom sorting order that treats `/` characters in special way.
	// Refer to this documentation for the specifics:
	// https://oxia-db.github.io/docs/features/oxia-key-sorting
	RangeScan(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...RangeScanOption) <-chan GetResult

	// GetSequenceUpdates allows to subscribe to the updates happening on a sequential key
	// The channel will report the current latest sequence for a given key.
	// Multiple updates can be collapsed into one single event with the
	// highest sequence.
	GetSequenceUpdates(ctx context.Context, prefixKey string, options ...GetSequenceUpdatesOption) (<-chan string, error)

	// GetNotifications creates a new subscription to receive the notifications
	// from Oxia for any change that is applied to the database
	GetNotifications() (Notifications, error)
}

// SyncClient is the main interface to perform operations with Oxia.
//
// Once a client instance is created, it will be valid until it gets explicitly
// closed, and it can be shared across different go-routines.
//
// If any ephemeral records are created (using the [Ephemeral] PutOption), they
// will all be automatically deleted when the client instance is closed, or
// if the process crashed.
type SyncClient interface {
	io.Closer

	// Put Associates a value with a key
	//
	// There are few options that can be passed to the Put operation:
	//  - The Put operation can be made conditional on that the record hasn't changed from
	//    a specific existing version by passing the [ExpectedVersionId] option.
	//  - Client can assert that the record does not exist by passing [ExpectedRecordNotExists]
	//  - Client can create an ephemeral record with [Ephemeral]
	//
	// Returns the actual key of the inserted record
	// Returns a [Version] object that contains information about the newly updated record
	// Returns [ErrorUnexpectedVersionId] if the expected version id does not match the
	// current version id of the record
	Put(ctx context.Context, key string, value []byte, options ...PutOption) (insertedKey string, version Version, err error)

	// Delete removes the key and its associated value from the data store.
	//
	// The Delete operation can be made conditional on that the record hasn't changed from
	// a specific existing version by passing the [ExpectedVersionId] option.
	// Returns [ErrorUnexpectedVersionId] if the expected version id does not match the
	// current version id of the record
	Delete(ctx context.Context, key string, options ...DeleteOption) error

	// DeleteRange deletes any records with keys within the specified range.
	// Note: Oxia uses a custom sorting order that treats `/` characters in special way.
	// Refer to this documentation for the specifics:
	// https://oxia-db.github.io/docs/features/oxia-key-sorting
	DeleteRange(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...DeleteRangeOption) error

	// Get returns the value associated with the specified key.
	// In addition to the value, a version object is also returned, with information
	// about the record state.
	// Returns ErrorKeyNotFound if the record does not exist
	Get(ctx context.Context, key string, options ...GetOption) (storedKey string, value []byte, version Version, err error)

	// List any existing keys within the specified range.
	// Note: Oxia uses a custom sorting order that treats `/` characters in special way.
	// Refer to this documentation for the specifics:
	// https://oxia-db.github.io/docs/features/oxia-key-sorting
	List(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...ListOption) (keys []string, err error)

	// RangeScan perform a scan for existing records with any keys within the specified range.
	// Ordering in results channel is respected only if a [PartitionKey] option is passed (and the keys were
	// inserted with that partition key).
	RangeScan(ctx context.Context, minKeyInclusive string, maxKeyExclusive string, options ...RangeScanOption) <-chan GetResult

	// GetSequenceUpdates allows to subscribe to the updates happening on a sequential key
	// The channel will report the current latest sequence for a given key.
	// Multiple updates can be collapsed into one single event with the
	// highest sequence.
	GetSequenceUpdates(ctx context.Context, prefixKey string, options ...GetSequenceUpdatesOption) (<-chan string, error)

	// GetNotifications creates a new subscription to receive the notifications
	// from Oxia for any change that is applied to the database
	GetNotifications() (Notifications, error)
}

// Version includes some information regarding the state of a record.
type Version struct {
	// VersionId represents an identifier that can be used to refer to a particular version
	// of a record.
	// Applications shouldn't make assumptions on the actual values of the VersionId. VersionIds
	// are only meaningful for a given key and don't reflect the number of changes that were made
	// on a given record.
	// Applications can use the VersionId when making [SyncClient.Put] or [SyncClient.Delete]
	// operations by passing an [ExpectedVersionId] option.
	VersionId int64

	// The time when the record was last created
	// (If the record gets deleted and recreated, it will have a new CreatedTimestamp value)
	CreatedTimestamp uint64

	// The time when the record was last modified
	ModifiedTimestamp uint64

	// The number of modifications to the record since it was last created
	// (If the record gets deleted and recreated, the ModificationsCount will restart at 0)
	ModificationsCount int64

	// Whether the record is ephemeral. See [Ephemeral]
	Ephemeral bool

	// For ephemeral records, the identifier of the session to which this record lifecycle
	// is attached to. Non-ephemeral records will always report 0.
	SessionId int64

	// For ephemeral records, the unique identity of the Oxia client that did last modify it.
	// It will be empty for all non-ephemeral records.
	ClientIdentity string
}

// PutResult structure is wrapping the version information for the result
// of a `Put` operation and an eventual error in the [AsyncClient].
type PutResult struct {
	// The Key of the inserted record
	Key string

	// The Version information
	Version Version

	// The error if the `Put` operation failed
	Err error
}

// GetResult structure is wrapping a record, with its Key and Value, its version information and
// an eventual error as results for a `Get` operation in the [AsyncClient].
type GetResult struct {
	// Key is the key of the record
	Key string

	// Value is the value of the record
	Value []byte

	// The version information
	Version Version

	// The error if the `Get` operation failed
	Err error
}

// ListResult structure is wrapping a list of keys, and a potential error as
// results for a `List` operation in the [AsyncClient].
type ListResult struct {
	// The list of keys returned by [List]
	Keys []string
	// The eventual error in the [List] operation
	Err error
}

// Notifications allow applications to receive the feed of changes
// that are happening in the Oxia database.
type Notifications interface {
	io.Closer

	// Ch exposes the channel where all the notification events are published
	Ch() <-chan *Notification
}

// NotificationType represents the type of the notification event.
type NotificationType int

const (
	// KeyCreated A record that didn't exist was created.
	KeyCreated NotificationType = iota
	// KeyModified An existing record was modified.
	KeyModified
	// KeyDeleted A record was deleted.
	KeyDeleted
	// KeyRangeRangeDeleted A range of keys was deleted.
	KeyRangeRangeDeleted
)

func (n NotificationType) String() string {
	switch n {
	case KeyCreated:
		return "KeyCreated"
	case KeyModified:
		return "KeyModified"
	case KeyDeleted:
		return "KeyDeleted"
	case KeyRangeRangeDeleted:
		return "KeyRangeRangeDeleted"
	}

	return "Unknown"
}

// Notification represents one change in the Oxia database.
type Notification struct {
	// The type of the modification
	Type NotificationType

	// The Key of the record to which the notification is referring
	Key string

	// The current VersionId of the record, or -1 for a KeyDeleted event
	VersionId int64

	// In case of a KeyRangeRangeDeleted notification, this would represent
	// the end (excluded) of the range of keys
	KeyRangeEnd string
}
