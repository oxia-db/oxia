# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Oxia is a distributed metadata store and coordination system written in Go, designed for large-scale distributed systems. It features a sharding architecture optimized for cloud-native applications.

## Build Commands

```bash
make build          # Build bin/oxia binary
make test           # Run all tests with coverage and race detection
make lint           # Run golangci-lint across all workspace modules
make proto          # Regenerate gRPC code from .proto files
make docker         # Build Docker image (oxia:latest)
make license-check  # Verify copyright headers
make license-format # Auto-format copyright headers
```

## Running a Single Test

```bash
go test -v -run TestName ./path/to/package/...
```

## Project Structure

The codebase uses Go workspaces with 5 modules:

- **cmd/** - CLI entry points (Cobra-based). Commands: `server`, `coordinator`, `client`, `admin`, `standalone`, `health`, `wal`, `perf`
- **common/** - Shared code: gRPC proto definitions (`common/proto/`), RPC utilities, metrics, security
- **oxia/** - Client library exposing `SyncClient` and `AsyncClient` interfaces
- **oxiad/** - Server implementations:
  - `coordinator/` - Cluster management, shard orchestration, leader election
  - `dataserver/` - Data storage using Pebble (LSM-tree KV store)
  - `dataserver/wal/` - Write-ahead log implementation
- **tests/** - Integration tests

## Architecture

```
Client (oxia/) ──gRPC──> Coordinator (oxiad/coordinator/)
                              │
                              ▼
                        Data Servers (oxiad/dataserver/)
                        [Sharded, Pebble-backed storage]
```

Key components:
- **Coordinator**: Manages cluster state, shard assignments, and leader balancing
- **Data Servers**: Handle actual data storage/retrieval with WAL for durability
- **Client Library**: Provides sync/async APIs with automatic request batching

## Code Style

- Line length limit: 120 characters
- Import grouping: stdlib, external, then `github.com/oxia-db/oxia` (enforced by goimports)
- All Go files require Apache 2.0 license header
- Cyclomatic complexity limit: 15
- Cognitive complexity limit: 25

## Proto Files

Protocol buffer definitions are in `common/proto/`. After modifying `.proto` files:
```bash
make proto
```

Requires: `protoc-gen-go`, `protoc-gen-go-grpc`, `protoc-gen-go-vtproto`

## Key Dependencies

- **CockroachDB Pebble**: LSM-tree key-value store for data persistence
- **gRPC + vtprotobuf**: Optimized RPC communication
- **Cobra/Viper**: CLI framework and configuration
- **OpenTelemetry**: Metrics and tracing
