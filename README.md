# p2p-dfs-node

[![Rust](https://github.com/ozankenangungor/p2p-dfs-node/actions/workflows/ci.yml/badge.svg)](https://github.com/ozankenangungor/p2p-dfs-node/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This project started as a small libp2p chat prototype and slowly turned into a proper DFS node. It stores files as content-addressed chunks, builds a Merkle root for each file, persists state in RocksDB, and moves metadata and chunk data over libp2p.

I wanted the networking side to feel like a daemon instead of a toy CLI, so the project is split into two parts:
- `p2p-dfs-node daemon` owns the swarm, storage, peer state, and download lifecycle.
- A local gRPC control plane handles user-facing commands such as `add`, `provide`, `get`, `status`, and `providing`.

## What it currently does

- Chunks files and addresses them by BLAKE3 hash plus Merkle root
- Stores manifests, raw chunks, provide state, and download progress in RocksDB
- Finds providers through Kademlia and can re-provide persisted content
- Serves metadata and chunks over bounded request/response protocols
- Supports optional gossipsub announcements for public files
- Tracks download progress, retries failed network work, and can resume from persisted chunk state

## Quick start

Build the binary:

```bash
cargo build --release
```

Start one daemon:

```bash
./target/release/p2p-dfs-node daemon \
  --listen-p2p /ip4/0.0.0.0/tcp/4001 \
  --grpc-addr 127.0.0.1:50051 \
  --db-path ./data/rocksdb \
  --key-file ./data/node_key.ed25519
```

A few details that matter:
- `--key-file` makes node identity stable across restarts.
- If the key file does not exist, the daemon creates it with private permissions.
- The gRPC control plane is local by default. Binding it to a non-loopback address now requires `--allow-remote-control`.
- `Ctrl+C` shuts down both the runtime loop and the gRPC server cleanly.

## Common workflow

All client commands talk to `http://127.0.0.1:50051` unless you override `--grpc-addr`.

```bash
# Add a local file and print its CID
p2p-dfs-node add ./file.bin

# Broadcast a public announcement for the file
p2p-dfs-node add ./file.bin --public

# Make the file discoverable through Kademlia
p2p-dfs-node provide <cid>

# Show which local CIDs are currently being re-provided
p2p-dfs-node providing

# Download a file by CID
p2p-dfs-node get <cid> -o ./downloads/file.bin

# Inspect local state
p2p-dfs-node list
p2p-dfs-node status
p2p-dfs-node peers

# Inspect or interrupt a download
p2p-dfs-node download-status <cid>
p2p-dfs-node cancel-download <cid>
```

One important note: `add --public` only publishes a gossipsub announcement. If you want other nodes to find the file through the DHT, you still need to run `provide`.

## How the daemon is structured

The runtime keeps strict ownership boundaries on purpose. `Swarm<NodeBehaviour>` lives in one task and is never shared across async workers. Anything that needs to talk to the network goes through a command channel.

The core pieces are:
- `NodeRuntime`, which owns the swarm, RocksDB handle, peer state, and pending request maps
- `NodeClient`, which sends commands into the runtime over `mpsc`
- gRPC handlers, which are thin wrappers around `NodeClient`
- download workers, which use `NodeClient` instead of touching libp2p directly

That keeps the swarm deterministic and avoids the usual mess of shared mutable state around networking code.

## Runtime flow

When you add a file:
1. The CLI sends `AddFile` over gRPC.
2. The runtime chunks the file, hashes each chunk, computes the Merkle root, stores everything in RocksDB, and returns the CID.
3. If `--public` was set, the daemon publishes a compact announcement.

When you provide a file:
1. The runtime verifies that the CID exists locally.
2. It starts a Kademlia provide query.
3. On success, the daemon persists provide state so it can re-provide after restart.

When you download a file:
1. The runtime starts a background download task and persists initial progress.
2. The task retries provider discovery until the DHT catches up or the retry budget is exhausted.
3. It fetches the manifest, validates the CID and Merkle root, then downloads missing chunks with bounded global and per-peer concurrency.
4. Each chunk is hashed again before it is accepted into local storage.
5. The file is assembled into a temporary path and atomically renamed into place.

## Protocols and storage

Transport and discovery:
- TCP + Noise + Yamux
- mDNS for local discovery
- Kademlia for provider discovery and re-providing

DFS protocols:
- `/dfs/metadata/1.0.0`
- `/dfs/chunk/1.0.0`

RocksDB column families:
- `meta`: `CID -> Manifest`
- `chunk`: `chunk_hash -> chunk bytes`
- `provide`: `CID -> provide state`
- `download`: `CID -> download progress`

## Guardrails

- No `unsafe`
- Untrusted network payloads are length-bounded and validated
- CID and chunk hash fields must be fixed-length hex
- RocksDB work is offloaded with `spawn_blocking`
- `provide` is rejected for content that is not stored locally
- Download cancellation now interrupts discovery, manifest fetch, chunk fetch retries, and assembly more predictably

## Development

```bash
cargo check --all-features
cargo test --all-features
cargo clippy --all-features -- -D warnings
```

The repository ships with CI for `check`, `fmt`, `clippy`, `test`, release builds, and `cargo audit`.
