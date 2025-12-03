# P2P Chat

[![Rust](https://github.com/ozankenangungor/p2p-chat/actions/workflows/ci.yml/badge.svg)](https://github.com/ozankenangungor/p2p-chat/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A peer-to-peer chat application built with [libp2p](https://libp2p.io/) in Rust.

## Features

- 🔐 **Secure Communication**: Uses Noise protocol for encrypted connections
- 🌐 **Peer-to-Peer**: Direct communication without central servers
- 🔍 **mDNS Discovery**: Automatic peer discovery on local networks
- 📢 **Broadcast Messaging**: Send messages to all connected peers
- 🏓 **Connection Health**: Automatic ping/pong for connection monitoring
- 📝 **JSON Protocol**: Structured message format for extensibility
- ⚡ **Async Runtime**: Built on Tokio for high performance
- 🔧 **Configurable**: CLI arguments and environment variables support

## Installation

### Prerequisites

- Rust 1.75 or higher
- Cargo

### From Source

```bash
# Clone the repository
git clone https://github.com/ozankenangungor/p2p-chat.git
cd p2p-chat

# Build the project
cargo build --release

# The binary will be at ./target/release/p2p-chat
```

## Usage

### Quick Start with mDNS (Recommended)

The easiest way to use P2P Chat is with mDNS auto-discovery. Just run the application on multiple machines on the same local network:

```bash
# Terminal 1 - Start first peer (random port)
p2p-chat

# Terminal 2 - Start second peer (random port)
p2p-chat

# They will automatically discover each other via mDNS!
```

### Starting a Peer (Listener)

Start a peer that will listen for connections:

```bash
# Using random OS-assigned port (default)
p2p-chat

# Using a specific port
p2p-chat --port 9999
```

### Connecting to a Peer

You can also manually connect to a specific peer:

```bash
# Connect to a peer directly
p2p-chat --port 9998 --peer /ip4/127.0.0.1/tcp/9999

# Or using environment variables
CHAT_P2P_PORT=9998 CHAT_PEER=/ip4/127.0.0.1/tcp/9999 p2p-chat
```

### Commands

Once running, you can use the following commands:

| Command   | Description                    |
|-----------|--------------------------------|
| `/help`   | Show available commands        |
| `/peers`  | List connected & discovered peers |
| `/status` | Show connection status         |
| `/quit`   | Exit the application           |

Type any other text to broadcast it to all connected peers.

### CLI Options

```
Usage: p2p-chat [OPTIONS]

Options:
  -p, --port <PORT>              Port to listen on (0 for random) [env: CHAT_P2P_PORT] [default: 0]
  -c, --peer <PEER>              Peer address to connect to [env: CHAT_PEER]
      --ping-interval <SECONDS>  Ping interval in seconds [default: 10]
      --idle-timeout <SECONDS>   Idle connection timeout in seconds [default: 30]
      --mdns <BOOL>              Enable mDNS for local network discovery [default: true]
  -v, --verbose                  Enable verbose logging
      --log-level <LEVEL>        Log level (trace, debug, info, warn, error) [default: info]
  -h, --help                     Print help
  -V, --version                  Print version
```

## How mDNS Discovery Works

```
┌─────────────────┐     mDNS Broadcast      ┌─────────────────┐
│   Application   │ ──────────────────────► │   Application   │
│      (Peer 1)   │                         │      (Peer 2)   │
│                 │ ◄────────────────────── │                 │
│  Broadcasts its │     mDNS Response       │  Discovers and  │
│   presence      │                         │   connects      │
└─────────────────┘                         └─────────────────┘
         │                                           │
         │              TCP Connection               │
         └───────────────────────────────────────────┘
                    Encrypted Chat Messages
```

1. Each peer broadcasts its presence via mDNS (multicast DNS)
2. Other peers on the same network discover the broadcast
3. Discovered peers automatically connect to each other
4. Messages are broadcast to all connected peers

## Architecture

```
src/
├── main.rs       # Application entry point and event loop
├── lib.rs        # Library exports
├── behaviour.rs  # libp2p network behaviour definitions
├── config.rs     # Configuration and CLI parsing
└── error.rs      # Error types
```

### Network Protocol

The application uses the following libp2p protocols:

- **Transport**: TCP with Noise encryption and Yamux multiplexing
- **Ping**: Connection health monitoring
- **mDNS**: Local network peer discovery (multicast DNS)
- **Request-Response**: JSON-based messaging protocol (`/p2p-chat/1.0.0`)

### Message Format

```json
// Request
{
  "message": "Hello, World!",
  "timestamp": 1703596800000
}

// Response
{
  "ack": true,
  "error": null
}
```

## Development

### Running Tests

```bash
cargo test
```

### Running with Debug Logging

```bash
RUST_LOG=debug cargo run -- --port 9999
```

### Code Formatting

```bash
cargo fmt
```

### Linting

```bash
cargo clippy -- -D warnings
```

## Docker (Optional)

```dockerfile
FROM rust:1.75-alpine AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM alpine:latest
COPY --from=builder /app/target/release/p2p-chat /usr/local/bin/
ENTRYPOINT ["p2p-chat"]
```

Build and run:

```bash
docker build -t p2p-chat .
docker run -it --rm -p 9999:9999 p2p-chat --port 9999
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [libp2p](https://libp2p.io/) - The modular peer-to-peer networking stack
- [Tokio](https://tokio.rs/) - Asynchronous runtime for Rust
