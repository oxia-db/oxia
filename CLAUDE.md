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

## Release Notes

Release notes are manually written. Do not rely on GitHub-generated release notes.

When preparing release notes ahead of time, copy `release-notes/TEMPLATE.md` to
`release-notes/<tag>.md` and fill it before creating the tag. The release workflow
uses this file as the GitHub release body when it exists. If the file is missing,
the workflow still creates the release so maintainers can add notes afterward.

Formatting rules:
- Do not include a redundant release-title line; GitHub already shows the tag.
- Use bold section labels like `**Compatibility**`, not Markdown headings.
- Do not include author names or generated `by @user in <url>` entries.
- Always consider compatibility, requirements, public API changes, metrics changes,
  and operational changes.
- Keep change bullets concise and include PR numbers, for example
  `dataserver: make authority validation configurable (#1136)`.
