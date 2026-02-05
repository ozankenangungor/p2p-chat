# Architecture

## Overview

The node is split into two major parts:

1. **Swarm Runtime (single owner)**
2. **gRPC Control Plane (command producer)**

The runtime owns `Swarm<NodeBehaviour>` and all network-side state. No other task directly touches `Swarm`.

## Runtime Ownership Model

`src/node.rs` implements a command bus pattern:

- `NodeRuntime` owns:
  - `Swarm<NodeBehaviour>`
  - `RocksStore`
  - peer/address tracking state
  - pending request correlation maps
- `NodeClient` (used by gRPC handlers and download tasks) sends `NodeCommand` messages over `mpsc`.
- Commands that need a response use `oneshot` channels.

This keeps networking deterministic and avoids concurrent mutable access to the swarm.

## libp2p Behaviour Stack

`NodeBehaviour` combines:

- `ping`
- `identify`
- `Toggle<mdns>`
- `kademlia`
- `request_response` for metadata
- `request_response` for chunks
- `Toggle<gossipsub>`

### Protocols

- Metadata protocol: `/dfs/metadata/1.0.0`
- Chunk protocol: `/dfs/chunk/1.0.0`

`src/protocol.rs` implements bounded framed codecs (bincode payload + explicit frame length checks).

## Storage Model (RocksDB)

`src/storage.rs` uses column families:

- `meta`: `CID -> Manifest`
- `chunk`: `chunk_hash -> raw chunk bytes`
- `provide`: `CID -> ProvideState`
- `download`: `CID -> DownloadProgress`

All DB work is done through `spawn_blocking` to avoid blocking the async runtime.

## Data Flow

### AddFile

1. CLI -> gRPC `AddFile`.
2. gRPC handler sends `NodeCommand::AddFile`.
3. Runtime chunks file, stores chunks + manifest, returns CID.
4. If `public=true`, runtime publishes a compact announcement (`cid`, `file_len`, `chunk_size`) via gossipsub.

### Provide

1. CLI -> gRPC `Provide`.
2. Runtime validates CID, calls `kademlia.start_providing`.
3. On success event, runtime persists provide state in RocksDB.

### Reprovide

A periodic timer reads `provide` CF and re-issues `start_providing` for stored CIDs.

### GetFile (Download)

1. CLI -> gRPC `GetFile`.
2. Runtime starts a download task with cancellation handle.
3. Download task uses `NodeClient` commands for all network operations:
   - `FindProviders`
   - `FetchManifest`
   - `FetchChunk`
4. Chunks are downloaded with bounded global and per-peer concurrency.
5. Each chunk hash is verified before persistence.
6. File is assembled from verified chunks and atomically renamed.
7. Progress is persisted in `download` CF for resume/status/cancel.

## Peer Management

Runtime tracks:

- connected peers
- discovered peers
- currently dialing peers
- last dial attempt timestamps
- listen addresses

Dialing protections:

- no self-dial
- no duplicate dial while connected/dialing
- cooldown between repeated dial attempts

## Shutdown

A shared shutdown signal is used by both runtime and gRPC server.

- `Ctrl+C` triggers graceful shutdown.
- Runtime loop exits cleanly.
- gRPC server stops via `serve_with_shutdown`.

## Test Strategy

- Unit tests cover:
  - mdns toggle
  - Merkle root correctness
  - protocol size limits
  - identity determinism
- Integration test (`tests/dfs_integration.rs`):
  - spins up two in-process nodes
  - node A adds/provides a file
  - node B discovers provider and downloads via metadata/chunk protocols
  - verifies downloaded bytes match source bytes
