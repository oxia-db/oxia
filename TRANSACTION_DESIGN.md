# Oxia Transaction Design

## Overview

This document outlines the design for adding transaction support to Oxia. The design supports both single-shard transactions (with partition key) and distributed transactions (without partition key) using a 2-phase commit protocol.

## High-Level Goals

1. **Client SDK Transaction API**: Provide intuitive transaction APIs for both sync and async clients
2. **Single-Shard Transactions**: Fast, local transactions when all keys belong to the same shard (via partition key)
3. **Distributed Transactions**: 2-phase commit protocol for transactions spanning multiple shards
4. **ACID Guarantees**: Full ACID semantics with snapshot isolation

## Client API Design

### 1. Transaction Interface

```go
// Transaction represents an Oxia transaction that can span one or more shards
type Transaction interface {
    // Put adds a put operation to the transaction
    Put(key string, value []byte, options ...PutOption) error

    // Delete adds a delete operation to the transaction
    Delete(key string, options ...DeleteOption) error

    // DeleteRange adds a delete range operation to the transaction
    DeleteRange(minKeyInclusive string, maxKeyExclusive string, options ...DeleteRangeOption) error

    // Get reads a value within the transaction (read your own writes)
    Get(ctx context.Context, key string, options ...GetOption) (string, []byte, Version, error)

    // Commit atomically commits all operations in the transaction
    Commit(ctx context.Context) error

    // Abort aborts the transaction and discards all pending operations
    Abort(ctx context.Context) error
}
```

### 2. Async Transaction Interface

```go
// AsyncTransaction represents an asynchronous transaction
type AsyncTransaction interface {
    // Put adds a put operation to the transaction
    Put(key string, value []byte, options ...PutOption) error

    // Delete adds a delete operation to the transaction
    Delete(key string, options ...DeleteOption) error

    // DeleteRange adds a delete range operation to the transaction
    DeleteRange(minKeyInclusive string, maxKeyExclusive string, options ...DeleteRangeOption) error

    // Get reads a value within the transaction
    Get(key string, options ...GetOption) <-chan GetResult

    // Commit atomically commits all operations
    Commit() <-chan error

    // Abort aborts the transaction
    Abort() <-chan error
}
```

### 3. Client Methods

```go
// SyncClient additions
type SyncClient interface {
    // ... existing methods ...

    // BeginTransaction starts a new transaction
    // If partitionKey is specified, creates a single-shard transaction
    // If partitionKey is nil, creates a distributed transaction
    BeginTransaction(ctx context.Context, options ...TransactionOption) (Transaction, error)
}

// AsyncClient additions
type AsyncClient interface {
    // ... existing methods ...

    // BeginTransaction starts a new async transaction
    BeginTransaction(options ...TransactionOption) (AsyncTransaction, error)
}
```

### 4. Transaction Options

```go
// TransactionOption configures transaction behavior
type TransactionOption interface {
    applyTransaction(opts *transactionOptions)
}

type transactionOptions struct {
    partitionKey *string
    timeout      time.Duration
    isolationLevel IsolationLevel
}

// IsolationLevel defines transaction isolation semantics
type IsolationLevel int

const (
    // SnapshotIsolation provides snapshot isolation (default)
    SnapshotIsolation IsolationLevel = iota
    // SerializableIsolation provides full serializability
    SerializableIsolation
)

// TransactionPartitionKey specifies this is a single-shard transaction
func TransactionPartitionKey(partitionKey string) TransactionOption

// TransactionTimeout sets a timeout for the transaction
func TransactionTimeout(timeout time.Duration) TransactionOption

// TransactionIsolationLevel sets the isolation level
func TransactionIsolationLevel(level IsolationLevel) TransactionOption
```

### 5. Example Usage

```go
// Single-shard transaction (fast path)
client, _ := oxia.NewSyncClient("localhost:6648")
txn, _ := client.BeginTransaction(ctx,
    oxia.TransactionPartitionKey("user:123"))

txn.Put("user:123:name", []byte("Alice"))
txn.Put("user:123:balance", []byte("100"))
txn.Delete("user:123:temp")

err := txn.Commit(ctx)

// Distributed transaction (2PC)
txn, _ := client.BeginTransaction(ctx)

txn.Put("account:alice:balance", []byte("50"))  // shard 1
txn.Put("account:bob:balance", []byte("150"))   // shard 2

err := txn.Commit(ctx) // 2-phase commit across shards
```

## Single-Shard Transaction Design

### Overview
When a partition key is specified, all operations are guaranteed to route to the same shard, enabling fast local transactions without distributed coordination.

### Implementation

1. **Transaction State**
   - Transaction ID (unique identifier)
   - List of pending operations (puts, deletes, delete-ranges)
   - Read set (for conflict detection with snapshot isolation)
   - Transaction timestamp (for snapshot reads)

2. **Write Path**
   - All operations are buffered locally in the client
   - On commit, send all operations in a single batch to the shard leader
   - Leader applies all operations atomically in a single WAL entry
   - Leader validates no conflicts have occurred (version checks)

3. **Read Path**
   - Reads within transaction see buffered writes (read-your-own-writes)
   - Reads to shard use transaction timestamp for snapshot consistency
   - Track read set for conflict detection

4. **Commit Protocol**
   ```
   Client                           Shard Leader
      |                                  |
      |--- TransactionCommit ----------->|
      |    (txn_id, operations)          |
      |                                  | Validate
      |                                  | Write to WAL
      |                                  | Apply to DB
      |<-- TransactionCommitResponse ----|
      |    (status, versions)            |
   ```

### Data Structures

```go
type singleShardTransaction struct {
    txnID        string
    shardID      int64
    partitionKey string
    operations   []txnOperation
    readSet      map[string]int64 // key -> version_id
    writeSet     map[string][]byte
    client       *clientImpl
    committed    bool
    aborted      bool
}

type txnOperation struct {
    opType OperationType
    key    string
    value  []byte
    expectedVersionId *int64
}

type OperationType int
const (
    OpPut OperationType = iota
    OpDelete
    OpDeleteRange
)
```

## Distributed Transaction Design (2-Phase Commit)

### Overview
When no partition key is specified, transactions may span multiple shards. We use a 2-phase commit protocol coordinated by the client.

### Roles

1. **Transaction Coordinator (Client)**: Orchestrates the 2PC protocol
2. **Participants (Shard Leaders)**: Execute operations and vote on commit

### Transaction States

```go
type TransactionState int
const (
    TxnActive TransactionState = iota
    TxnPreparing
    TxnPrepared
    TxnCommitting
    TxnCommitted
    TxnAborting
    TxnAborted
)
```

### 2-Phase Commit Protocol

#### Phase 1: Prepare

```
Client (Coordinator)              Shard 1               Shard 2               Shard N
      |                              |                     |                     |
      |--- Prepare(txn_id, ops) ---->|                     |                     |
      |--- Prepare(txn_id, ops) ----------------------->  |                     |
      |--- Prepare(txn_id, ops) -------------------------------------------->  |
      |                              |                     |                     |
      |                              | Write to WAL        | Write to WAL        | Write to WAL
      |                              | Lock keys           | Lock keys           | Lock keys
      |                              | Validate            | Validate            | Validate
      |                              |                     |                     |
      |<-- Prepared (vote: yes) -----|                     |                     |
      |<-- Prepared (vote: yes) -------------------------|                     |
      |<-- Prepared (vote: yes) <--------------------------------------------|
      |                              |                     |                     |
```

If all participants vote YES, proceed to Phase 2. If any vote NO or timeout, abort.

#### Phase 2: Commit

```
Client (Coordinator)              Shard 1               Shard 2               Shard N
      |                              |                     |                     |
      |--- Commit(txn_id) ---------->|                     |                     |
      |--- Commit(txn_id) -------------------------->     |                     |
      |--- Commit(txn_id) ------------------------------------------------>     |
      |                              |                     |                     |
      |                              | Apply to DB         | Apply to DB         | Apply to DB
      |                              | Unlock keys         | Unlock keys         | Unlock keys
      |                              | Write committed     | Write committed     | Write committed
      |                              |                     |                     |
      |<-- Committed -----------------|                     |                     |
      |<-- Committed -------------------------------|     |                     |
      |<-- Committed <---------------------------------------------------|     |
      |                              |                     |                     |
```

#### Phase 2: Abort (if any participant votes NO)

```
Client (Coordinator)              Shard 1               Shard 2               Shard N
      |                              |                     |                     |
      |--- Abort(txn_id) ----------->|                     |                     |
      |--- Abort(txn_id) -------------------------->      |                     |
      |--- Abort(txn_id) ------------------------------------------------>      |
      |                              |                     |                     |
      |                              | Discard changes     | Discard changes     | Discard changes
      |                              | Unlock keys         | Unlock keys         | Unlock keys
      |                              |                     |                     |
      |<-- Aborted -------------------|                     |                     |
      |<-- Aborted ------------------------------|      |                     |
      |<-- Aborted <--------------------------------------------------|      |
      |                              |                     |                     |
```

### Transaction Coordinator State

The client maintains transaction state for recovery:

```go
type distributedTransaction struct {
    txnID          string
    operations     map[int64][]txnOperation // shardID -> operations
    state          TransactionState
    participants   map[int64]ParticipantState
    prepareTimeout time.Duration
    commitTimeout  time.Duration
    client         *clientImpl
}

type ParticipantState struct {
    shardID  int64
    state    TransactionState
    leader   string
}
```

### Failure Handling

1. **Client Failure During Prepare**:
   - Participants timeout waiting for commit/abort decision
   - After timeout, participants abort the transaction
   - Prepared transactions are stored in WAL for recovery

2. **Client Failure After Prepare**:
   - Coordinator writes prepare outcome to local transaction log
   - On restart, client queries participants for transaction status
   - Client continues with commit or abort based on recovery

3. **Participant Failure**:
   - During Prepare: Coordinator treats as NO vote, aborts transaction
   - During Commit: Coordinator retries commit until success
   - Transaction remains in participant's WAL for recovery

4. **Network Partition**:
   - Prepare phase times out, transaction aborted
   - After partition heals, coordinator sends abort to all participants

### Transaction Recovery

Each shard maintains a transaction table:

```go
type TransactionRecord struct {
    TxnID       string
    State       TransactionState
    Operations  []txnOperation
    PreparedAt  time.Time
    Locks       []string // locked keys
}
```

Recovery process:
1. On shard leader startup, scan transaction table
2. For prepared transactions older than timeout: abort
3. For committed transactions: ensure applied to DB
4. Clean up completed transactions

## Protocol Buffers

### New Messages

```protobuf
// Transaction prepare request
message TransactionPrepareRequest {
  string txn_id = 1;
  int64 shard = 2;
  repeated PutRequest puts = 3;
  repeated DeleteRequest deletes = 4;
  repeated DeleteRangeRequest delete_ranges = 5;
  fixed64 prepare_timestamp = 6;
}

message TransactionPrepareResponse {
  enum Vote {
    YES = 0;
    NO = 1;
  }
  Vote vote = 1;
  Status status = 2;
  string reason = 3; // If NO vote, reason for rejection
}

// Transaction commit request
message TransactionCommitRequest {
  string txn_id = 1;
  int64 shard = 2;
}

message TransactionCommitResponse {
  Status status = 1;
  repeated PutResponse puts = 2;
  repeated DeleteResponse deletes = 3;
  repeated DeleteRangeResponse delete_ranges = 4;
}

// Transaction abort request
message TransactionAbortRequest {
  string txn_id = 1;
  int64 shard = 2;
}

message TransactionAbortResponse {
  Status status = 1;
}

// Single-shard transaction commit (optimized)
message SingleShardTransactionRequest {
  string txn_id = 1;
  int64 shard = 2;
  repeated PutRequest puts = 3;
  repeated DeleteRequest deletes = 4;
  repeated DeleteRangeRequest delete_ranges = 5;
}

message SingleShardTransactionResponse {
  Status status = 1;
  repeated PutResponse puts = 2;
  repeated DeleteResponse deletes = 3;
  repeated DeleteRangeResponse delete_ranges = 4;
}
```

### RPC Service Additions

```protobuf
service OxiaClient {
  // ... existing RPCs ...

  // Single-shard transaction (optimized path)
  rpc SingleShardTransaction(SingleShardTransactionRequest)
      returns (SingleShardTransactionResponse);

  // Distributed transaction prepare phase
  rpc TransactionPrepare(TransactionPrepareRequest)
      returns (TransactionPrepareResponse);

  // Distributed transaction commit phase
  rpc TransactionCommit(TransactionCommitRequest)
      returns (TransactionCommitResponse);

  // Distributed transaction abort
  rpc TransactionAbort(TransactionAbortRequest)
      returns (TransactionAbortResponse);
}
```

## Data Server Implementation

### WAL Entry Types

Add new entry types for transactions:

```go
const (
    // ... existing entry types ...
    EntryTypeTransactionPrepare = 10
    EntryTypeTransactionCommit = 11
    EntryTypeTransactionAbort = 12
)
```

### Transaction Manager on Data Server

```go
type TransactionManager interface {
    // Prepare a transaction
    Prepare(ctx context.Context, req *proto.TransactionPrepareRequest) (*proto.TransactionPrepareResponse, error)

    // Commit a prepared transaction
    Commit(ctx context.Context, req *proto.TransactionCommitRequest) (*proto.TransactionCommitResponse, error)

    // Abort a prepared transaction
    Abort(ctx context.Context, req *proto.TransactionAbortRequest) (*proto.TransactionAbortResponse, error)

    // Execute single-shard transaction
    ExecuteSingleShard(ctx context.Context, req *proto.SingleShardTransactionRequest) (*proto.SingleShardTransactionResponse, error)

    // Recover in-flight transactions on startup
    RecoverTransactions() error
}
```

### Lock Management

Implement a lock manager for transaction isolation:

```go
type LockManager interface {
    // AcquireReadLock acquires a read lock on a key
    AcquireReadLock(txnID string, key string) error

    // AcquireWriteLock acquires a write lock on a key
    AcquireWriteLock(txnID string, key string) error

    // ReleaseAll releases all locks held by a transaction
    ReleaseAll(txnID string) error

    // CheckConflict checks if there's a conflict with existing locks
    CheckConflict(txnID string, key string, isWrite bool) error
}
```

### Database Changes

Extend the DB interface to support transactions:

```go
type DB interface {
    // ... existing methods ...

    // PrepareTransaction prepares a transaction
    PrepareTransaction(txnID string, operations []Operation) error

    // CommitTransaction commits a prepared transaction
    CommitTransaction(txnID string) error

    // AbortTransaction aborts a prepared transaction
    AbortTransaction(txnID string) error

    // GetTransaction retrieves transaction state
    GetTransaction(txnID string) (*TransactionRecord, error)
}
```

Store prepared transactions in a separate column family in Pebble:

```
cf:txn/{txn_id} -> TransactionRecord (serialized)
```

## Concurrency Control

### Snapshot Isolation (Default)

1. Each transaction gets a start timestamp
2. Reads see a consistent snapshot as of start timestamp
3. Writes are validated at commit time:
   - Check if any read keys have been modified since start timestamp
   - Check if any write keys are locked by other transactions
4. If validation succeeds, transaction commits with new timestamp

### Serializable Isolation (Optional)

1. Use predicate locks for range queries
2. Validate both read and write sets against all concurrent transactions
3. More restrictive but provides full serializability

## Performance Considerations

### Optimizations

1. **Single-Shard Fast Path**:
   - No 2PC overhead
   - Single WAL entry
   - Minimal locking

2. **Batch Prepare Messages**:
   - Send prepare requests to all participants in parallel
   - Use connection pooling

3. **Early Lock Release**:
   - For read-only transactions, no locks needed
   - For single-shard, locks released immediately after commit

4. **Transaction Caching**:
   - Cache prepared transactions in memory
   - Reduce WAL reads on commit/abort

5. **Lock Escalation**:
   - Start with key-level locks
   - Escalate to range locks if too many keys locked

### Monitoring Metrics

```go
// Metrics to track
- oxia_transaction_duration_seconds (histogram)
- oxia_transaction_prepare_duration_seconds (histogram)
- oxia_transaction_commit_duration_seconds (histogram)
- oxia_transaction_abort_total (counter)
- oxia_transaction_conflict_total (counter)
- oxia_transaction_timeout_total (counter)
- oxia_transaction_active_count (gauge)
- oxia_transaction_locks_held (gauge)
```

## Error Handling

### Error Types

```go
var (
    // ErrTransactionConflict indicates a write-write conflict
    ErrTransactionConflict = errors.New("transaction conflict")

    // ErrTransactionTimeout indicates transaction timed out
    ErrTransactionTimeout = errors.New("transaction timeout")

    // ErrTransactionAborted indicates transaction was aborted
    ErrTransactionAborted = errors.New("transaction aborted")

    // ErrTransactionNotFound indicates transaction not found (already committed/aborted)
    ErrTransactionNotFound = errors.New("transaction not found")

    // ErrInvalidTransactionState indicates operation not allowed in current state
    ErrInvalidTransactionState = errors.New("invalid transaction state")
)
```

## Testing Strategy

### Unit Tests
- Transaction state machine
- Lock manager
- Conflict detection
- Timeout handling

### Integration Tests
- Single-shard transactions
- Distributed transactions
- Failure injection (client crashes, network partitions)
- Concurrent transactions
- Recovery scenarios

### Stress Tests
- High concurrency
- Large transactions
- Mixed workload (transactions + regular operations)

## Implementation Phases

### Phase 1: Foundation
1. Add transaction proto definitions
2. Implement transaction state machines (client & server)
3. Add WAL support for transaction entries
4. Basic lock manager

### Phase 2: Single-Shard Transactions
1. Client API implementation
2. Server-side transaction manager
3. Single-shard commit protocol
4. Unit and integration tests

### Phase 3: Distributed Transactions
1. Implement 2PC coordinator in client
2. Server-side prepare/commit/abort handlers
3. Transaction recovery logic
4. Distributed transaction tests

### Phase 4: Optimization & Production
1. Performance tuning
2. Monitoring and metrics
3. Failure injection testing
4. Documentation and examples

## Migration Strategy

### Backward Compatibility
- All existing APIs remain unchanged
- Transactions are opt-in feature
- No changes to existing data format
- Wire protocol versioning for new RPC methods

### Rollout Plan
1. Deploy server changes (backward compatible)
2. Update client libraries with transaction support
3. Enable feature flag for testing
4. Gradual rollout to production
5. Monitor metrics and error rates

## Open Questions

1. **Transaction ID Generation**: UUID vs timestamp-based?
2. **Transaction Timeout Defaults**: What are reasonable defaults?
3. **Max Transaction Size**: Should we limit number of operations per transaction?
4. **Read-Only Transactions**: Should we have a separate optimized path?
5. **Nested Transactions**: Support for savepoints?
6. **Cross-Namespace Transactions**: Should we support this?

## Future Enhancements

1. **Optimistic Concurrency Control**: Alternative to locking
2. **Distributed Transaction Log**: Centralized transaction coordinator
3. **Transaction Priorities**: High-priority transactions preempt low-priority
4. **Adaptive Timeouts**: Adjust timeouts based on transaction history
5. **Read-Only Optimization**: Skip prepare phase for read-only transactions
