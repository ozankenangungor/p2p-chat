use libp2p::futures::StreamExt;
use libp2p::mdns;
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
    peers: HashMap<PeerId, Vec<Multiaddr>>,
    discovered_peers: HashMap<PeerId, Vec<Multiaddr>>,
}

impl AppState {
    fn new() -> Self {
        Self {
            peers: HashMap::new(),
            discovered_peers: HashMap::new(),
        }
    }

    fn add_peer(&mut self, peer_id: PeerId, addr: Multiaddr) {
        self.peers.entry(peer_id).or_default().push(addr);
        self.discovered_peers.remove(&peer_id);
        info!("Peer added: {} (total: {})", peer_id, self.peers.len());
    }

    fn remove_peer(&mut self, peer_id: &PeerId) {
        self.peers.remove(peer_id);
        info!(
            "Peer removed: {} (remaining: {})",
            peer_id,
            self.peers.len()
        );
    }

    fn add_discovered_peer(&mut self, peer_id: PeerId, addr: Multiaddr) {
        if !self.peers.contains_key(&peer_id) {
            self.discovered_peers.entry(peer_id).or_default().push(addr);
        }
    }

    fn remove_discovered_peer(&mut self, peer_id: &PeerId) {
        self.discovered_peers.remove(peer_id);
    }

    fn connected_peer_ids(&self) -> Vec<PeerId> {
        self.peers.keys().copied().collect()
    }

    fn peer_count(&self) -> usize {
        self.peers.len()
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
        .with_behaviour(|key_pair| {
            let local_peer_id = key_pair.public().to_peer_id();
            ChatBehaviour::new(local_peer_id, config.ping_interval_duration())
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(config.idle_timeout_duration()))
        .build();

    Ok(swarm)
}

fn handle_swarm_event(
    event: SwarmEvent<ChatBehaviourEvent>,
    swarm: &mut libp2p::Swarm<ChatBehaviour>,
    state: &mut AppState,
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
            state.add_peer(peer_id, addr);
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
            handle_behaviour_event(event, swarm, state);
        }

        _ => {
            debug!("Unhandled swarm event");
        }
    }
}

fn handle_behaviour_event(
    event: ChatBehaviourEvent,
    swarm: &mut libp2p::Swarm<ChatBehaviour>,
    state: &mut AppState,
) {
    match event {
        ChatBehaviourEvent::Ping(ping_event) => {
            debug!("🏓 Ping event: {:?}", ping_event);
        }

        ChatBehaviourEvent::Mdns(mdns_event) => {
            handle_mdns_event(mdns_event, swarm, state);
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

fn handle_mdns_event(
    event: mdns::Event,
    swarm: &mut libp2p::Swarm<ChatBehaviour>,
    state: &mut AppState,
) {
    match event {
        mdns::Event::Discovered(peers) => {
            for (peer_id, addr) in peers {
                info!("🔍 mDNS discovered peer: {} at {}", peer_id, addr);
                state.add_discovered_peer(peer_id, addr.clone());
                swarm.add_peer_address(peer_id, addr.clone());
                if swarm.dial(addr.clone()).is_ok() {
                    debug!("Dialing discovered peer: {}", peer_id);
                }
            }
        }
        mdns::Event::Expired(peers) => {
            for (peer_id, addr) in peers {
                debug!("🔍 mDNS peer expired: {} at {}", peer_id, addr);
                state.remove_discovered_peer(&peer_id);
            }
        }
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

    if message.starts_with('/') {
        handle_command(&message, state, swarm);
        return;
    }

    let connected_peers = state.connected_peer_ids();

    if connected_peers.is_empty() {
        warn!("⚠️  No peers connected. Waiting for connections...");
        return;
    }

    let request = MessageRequest::new(message.clone());
    let mut sent_count = 0;

    for peer_id in &connected_peers {
        swarm
            .behaviour_mut()
            .messaging
            .send_request(peer_id, request.clone());
        sent_count += 1;
    }

    info!(
        "💬 [{}]: {} (sent to {} peer{})",
        local_peer_id,
        message,
        sent_count,
        if sent_count == 1 { "" } else { "s" }
    );
}

fn handle_command(command: &str, state: &AppState, swarm: &libp2p::Swarm<ChatBehaviour>) {
    let parts: Vec<&str> = command.split_whitespace().collect();

    match parts.first().copied() {
        Some("/help") => {
            println!("\n📚 Available commands:");
            println!("  /help     - Show this help message");
            println!("  /peers    - List connected peers");
            println!("  /status   - Show connection status");
            println!("  /quit     - Exit the application\n");
        }
        Some("/peers") => {
            println!("\n👥 Connected peers ({}):", state.peer_count());
            if state.peers.is_empty() {
                println!("  No peers connected");
            } else {
                for (peer_id, addrs) in &state.peers {
                    println!("  • {}", peer_id);
                    for addr in addrs {
                        println!("    └── {}", addr);
                    }
                }
            }

            if !state.discovered_peers.is_empty() {
                println!("\n🔍 Discovered peers (not connected):");
                for (peer_id, addrs) in &state.discovered_peers {
                    println!("  • {}", peer_id);
                    for addr in addrs {
                        println!("    └── {}", addr);
                    }
                }
            }
            println!();
        }
        Some("/status") => {
            let connected = state.peer_count();
            let discovered = state.discovered_peers.len();
            let external_addrs: Vec<_> = swarm.external_addresses().collect();

            println!("\n📊 Status:");
            println!("  Connected peers: {}", connected);
            println!("  Discovered peers: {}", discovered);
            println!("  External addresses: {}", external_addrs.len());
            for addr in external_addrs {
                println!("    └── {}", addr);
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

fn print_banner(local_peer_id: &PeerId, listen_addr: &str, mdns_enabled: bool) {
    let mdns_status = if mdns_enabled { "enabled" } else { "disabled" };

    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║                    P2P Chat Application                   ║");
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  Peer ID: {}  ║", &local_peer_id.to_string()[..46]);
    println!("║  Listen:  {:<47} ║", listen_addr);
    println!("║  mDNS:    {:<47} ║", mdns_status);
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  Commands: /help, /peers, /status, /quit                 ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse_args();
    init_logging(&config);

    info!("Starting P2P Chat...");
    if config.mdns {
        info!("mDNS peer discovery enabled");
    }

    let mut swarm = build_swarm(&config)?;
    let local_peer_id = *swarm.local_peer_id();

    let listen_addr: Multiaddr = config.listen_addr().parse()?;
    swarm.listen_on(listen_addr)?;

    if let Some(ref peer) = config.peer {
        info!("Dialing peer: {}", peer);
        swarm.dial(peer.clone())?;
    }

    print_banner(&local_peer_id, &config.listen_addr(), config.mdns);

    let mut state = AppState::new();
    let mut stdin = BufReader::new(io::stdin()).lines();

    loop {
        select! {
            event = swarm.select_next_some() => {
                handle_swarm_event(event, &mut swarm, &mut state);
            }

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

            _ = signal::ctrl_c() => {
                info!("👋 Received Ctrl+C, shutting down gracefully...");
                break;
            }
        }
    }

    info!("P2P Chat shutdown complete");
    Ok(())
}
