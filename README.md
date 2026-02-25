# p2p-chat

[![Rust](https://github.com/ozankenangungor/p2p-chat/actions/workflows/ci.yml/badge.svg)](https://github.com/ozankenangungor/p2p-chat/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This repository started as a small libp2p chat experiment. It now runs as a DFS node: files are chunked, hashed, addressed by Merkle root, and exchanged over libp2p.

The app has two sides:
- a daemon (`p2p-chat daemon`) that owns networking and storage
- a local gRPC control plane that CLI commands talk to (`add`, `provide`, `get`, `status`, ...)

## What you get

At a high level:
- Content-addressed file storage (BLAKE3 chunk hashes + Merkle root CID)
- RocksDB persistence with separate column families for metadata, chunks, provide state, and download state
- Kademlia provider discovery/providing
- Request/response protocols for metadata and chunk transfer
- Optional gossipsub announcements for public files
- Download orchestration with bounded concurrency, retries, and resumable progress

## Quick start

Build:

```bash
cargo build --release
```

Run a daemon:

```bash
./target/release/p2p-chat daemon \
  --listen-p2p /ip4/0.0.0.0/tcp/4001 \
  --grpc-addr 127.0.0.1:50051 \
  --db-path ./data/rocksdb \
  --key-file ./data/node_key.ed25519
```

Notes:
- `--key-file` gives you deterministic node identity.
- If the key file is missing, it is generated and written with private permissions.
- `Ctrl+C` triggers graceful shutdown (runtime + gRPC server).

## CLI workflow

All client commands connect to local gRPC by default (`http://127.0.0.1:50051`).

```bash
# Add a local file, print CID
p2p-chat add ./file.bin

# Add and announce (if daemon has announcements enabled)
p2p-chat add ./file.bin --public

# Advertise that this node provides CID
p2p-chat provide <cid>

# Download a file by CID
p2p-chat get <cid> -o ./downloads/file.bin

# Local state
p2p-chat list
p2p-chat status
p2p-chat peers

# Download lifecycle
p2p-chat download-status <cid>
p2p-chat cancel-download <cid>
```

If your daemon is on a different local port:

```bash
p2p-chat status --grpc-addr http://127.0.0.1:60051
```

## Networking and storage details

- Transport: TCP + Noise + Yamux
- Discovery: mDNS (toggleable) + Kademlia
- DFS protocols:
  - `/dfs/metadata/1.0.0`
  - `/dfs/chunk/1.0.0`
- RocksDB column families:
  - `meta` (CID -> manifest)
  - `chunk` (chunk_hash -> bytes)
  - `provide` (CID -> provide state)
  - `download` (CID -> progress)

## Safety and hardening

- No `unsafe` code
- Network payloads are handled as untrusted input
- Protocol codecs enforce strict frame limits
- CID/chunk hash fields are validated as fixed-length hex
- RocksDB access is offloaded with `spawn_blocking`
- Swarm is single-owner and controlled through a command channel (no shared mutable swarm across tasks)

## Development

```bash
cargo check --all-features
cargo test --all-features
cargo clippy --all-features -- -D warnings
```

## Architecture

For a deeper runtime/protocol breakdown, see [ARCHITECTURE.md](ARCHITECTURE.md).
