use libp2p::futures::StreamExt;
use libp2p::request_response::{self, Message};
use libp2p::swarm::SwarmEvent;
use libp2p::{noise, tcp, yamux, Multiaddr, PeerId};
use p2p_chat::{
    behaviour::{ChatBehaviour, ChatBehaviourEvent, MessageRequest, MessageResponse},
    config::Config,
};
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::{io, select, signal};
use tracing::{debug, error, info, warn};

struct AppState {
    /// Currently connected peers with their addresses
    peers: HashMap<PeerId, Vec<Multiaddr>>,
    /// The active peer we're chatting with
    active_peer: Option<PeerId>,
}

impl AppState {
    fn new() -> Self {
        Self {
            peers: HashMap::new(),
            active_peer: None,
        }
    }

    fn add_peer(&mut self, peer_id: PeerId, addr: Multiaddr) {
        self.peers.entry(peer_id).or_default().push(addr);
        if self.active_peer.is_none() {
            self.active_peer = Some(peer_id);
            info!("Active peer set to: {}", peer_id);
        }
    }

    fn remove_peer(&mut self, peer_id: &PeerId) {
        self.peers.remove(peer_id);
        if self.active_peer.as_ref() == Some(peer_id) {
            self.active_peer = self.peers.keys().next().copied();
            if let Some(new_active) = self.active_peer {
                info!("Active peer changed to: {}", new_active);
            } else {
                info!("No active peer available");
            }
        }
    }
}

fn init_logging(config: &Config) {
    let filter = config.log_filter();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&filter)),
        )
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    debug!("Logging initialized with filter: {}", filter);
}

fn build_swarm(config: &Config) -> anyhow::Result<libp2p::Swarm<ChatBehaviour>> {
    let swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|_key_pair| Ok(ChatBehaviour::new(config.ping_interval_duration())))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(config.idle_timeout_duration()))
        .build();

    Ok(swarm)
}

fn handle_swarm_event(
    event: SwarmEvent<ChatBehaviourEvent>,
    swarm: &mut libp2p::Swarm<ChatBehaviour>,
    state: &mut AppState,
    peer_addr: Option<&Multiaddr>,
) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            info!("📡 Listening on {}", address);
        }

        SwarmEvent::ConnectionEstablished {
            peer_id, endpoint, ..
        } => {
            let addr = endpoint.get_remote_address().clone();
            info!("✅ Connected to peer: {} at {}", peer_id, addr);
            state.add_peer(peer_id, addr.clone());

            // Add the peer address for future connections
            if let Some(peer) = peer_addr {
                swarm.add_peer_address(peer_id, peer.clone());
            }
        }

        SwarmEvent::ConnectionClosed {
            peer_id,
            cause,
            num_established,
            ..
        } => {
            if num_established == 0 {
                warn!(
                    "❌ Disconnected from peer: {} (cause: {:?})",
                    peer_id, cause
                );
                state.remove_peer(&peer_id);
            }
        }

        SwarmEvent::IncomingConnection {
            local_addr,
            send_back_addr,
            ..
        } => {
            debug!(
                "📥 Incoming connection from {} to {}",
                send_back_addr, local_addr
            );
        }

        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
            error!(
                "🚫 Failed to connect to {:?}: {}",
                peer_id.map(|p| p.to_string()).unwrap_or_default(),
                error
            );
        }

        SwarmEvent::Behaviour(event) => {
            handle_behaviour_event(event, swarm);
        }

        _ => {
            debug!("Unhandled swarm event");
        }
    }
}

fn handle_behaviour_event(event: ChatBehaviourEvent, swarm: &mut libp2p::Swarm<ChatBehaviour>) {
    match event {
        ChatBehaviourEvent::Ping(ping_event) => {
            debug!("🏓 Ping event: {:?}", ping_event);
        }

        ChatBehaviourEvent::Messaging(msg_event) => match msg_event {
            request_response::Event::Message { peer, message } => match message {
                Message::Request {
                    request, channel, ..
                } => {
                    info!("💬 [{}]: {}", peer, request.message);

                    if let Err(e) = swarm
                        .behaviour_mut()
                        .messaging
                        .send_response(channel, MessageResponse::ok())
                    {
                        error!("Failed to send response: {:?}", e);
                    }
                }
                Message::Response { response, .. } => {
                    if response.ack {
                        debug!("✓ Message acknowledged by {}", peer);
                    } else {
                        warn!(
                            "✗ Message not acknowledged by {}: {:?}",
                            peer, response.error
                        );
                    }
                }
            },

            request_response::Event::OutboundFailure {
                peer,
                error,
                request_id,
                ..
            } => {
                error!(
                    "📤 Outbound failure to {} (request {:?}): {}",
                    peer, request_id, error
                );
            }

            request_response::Event::InboundFailure {
                peer,
                error,
                request_id,
                ..
            } => {
                error!(
                    "📥 Inbound failure from {} (request {:?}): {}",
                    peer, request_id, error
                );
            }

            request_response::Event::ResponseSent { peer, request_id } => {
                debug!("Response sent to {} for request {:?}", peer, request_id);
            }
        },
    }
}

fn send_message(
    swarm: &mut libp2p::Swarm<ChatBehaviour>,
    state: &AppState,
    message: String,
    local_peer_id: &PeerId,
) {
    if message.trim().is_empty() {
        return;
    }

    // Handle special commands
    if message.starts_with('/') {
        handle_command(&message, state);
        return;
    }

    if let Some(peer_id) = state.active_peer {
        let request = MessageRequest::new(message.clone());
        swarm
            .behaviour_mut()
            .messaging
            .send_request(&peer_id, request);
        info!("💬 [{}]: {}", local_peer_id, message);
    } else {
        warn!("⚠️  No peer connected. Waiting for connections...");
    }
}

fn handle_command(command: &str, state: &AppState) {
    let parts: Vec<&str> = command.split_whitespace().collect();

    match parts.first().copied() {
        Some("/help") => {
            println!("\n📚 Available commands:");
            println!("  /help     - Show this help message");
            println!("  /peers    - List connected peers");
            println!("  /quit     - Exit the application\n");
        }
        Some("/peers") => {
            println!("\n👥 Connected peers:");
            if state.peers.is_empty() {
                println!("  No peers connected");
            } else {
                for (peer_id, addrs) in &state.peers {
                    let active = if state.active_peer.as_ref() == Some(peer_id) {
                        " (active)"
                    } else {
                        ""
                    };
                    println!("  • {}{}", peer_id, active);
                    for addr in addrs {
                        println!("    └── {}", addr);
                    }
                }
            }
            println!();
        }
        Some("/quit") => {
            info!("👋 Goodbye!");
            std::process::exit(0);
        }
        _ => {
            println!("❓ Unknown command. Type /help for available commands.");
        }
    }
}

fn print_banner(local_peer_id: &PeerId, listen_addr: &str) {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║                    P2P Chat Application                   ║");
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  Your Peer ID: {}  ║", &local_peer_id.to_string()[..46]);
    println!("║  Listening on: {}                       ║", listen_addr);
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  Commands: /help, /peers, /quit                          ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse configuration
    let config = Config::parse_args();

    // Initialize logging
    init_logging(&config);

    info!("Starting P2P Chat...");

    // Build swarm
    let mut swarm = build_swarm(&config)?;
    let local_peer_id = *swarm.local_peer_id();

    // Start listening
    let listen_addr: Multiaddr = config.listen_addr().parse()?;
    swarm.listen_on(listen_addr)?;

    // Connect to peer if specified
    let peer_addr = config.peer.clone();
    if let Some(ref peer) = peer_addr {
        info!("Dialing peer: {}", peer);
        swarm.dial(peer.clone())?;
    }

    // Print welcome banner
    print_banner(&local_peer_id, &config.listen_addr());

    // Initialize state
    let mut state = AppState::new();

    // Setup stdin reader
    let mut stdin = BufReader::new(io::stdin()).lines();

    // Main event loop
    loop {
        select! {
            // Handle swarm events
            event = swarm.select_next_some() => {
                handle_swarm_event(event, &mut swarm, &mut state, peer_addr.as_ref());
            }

            // Handle user input
            line = stdin.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        send_message(&mut swarm, &state, line, &local_peer_id);
                    }
                    Ok(None) => {
                        info!("👋 EOF received, shutting down...");
                        break;
                    }
                    Err(e) => {
                        error!("Error reading stdin: {}", e);
                    }
                }
            }

            // Handle shutdown signals
            _ = signal::ctrl_c() => {
                info!("👋 Received Ctrl+C, shutting down gracefully...");
                break;
            }
        }
    }

    info!("P2P Chat shutdown complete");
    Ok(())
}
