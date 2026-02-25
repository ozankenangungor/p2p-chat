use crate::behaviour::{NodeBehaviour, NodeBehaviourEvent};
use crate::config::DaemonArgs;
use crate::download::{run_download, DownloadConfig};
use crate::identity::load_or_create_identity;
use crate::manifest::Manifest;
use crate::protocol::{
    chunk_bytes, chunk_response_from_bytes, ChunkRequest, ChunkResponse, MetadataRequest,
    MetadataResponse,
};
use crate::storage::{
    now_unix_ms, DownloadPhase, DownloadProgress, ProvideState, RocksStorageConfig, RocksStore,
};
use crate::validation::{validate_chunk_hash_hex, validate_cid_hex};
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use libp2p::futures::StreamExt;
use libp2p::gossipsub;
use libp2p::kad::{self, QueryId, QueryResult};
use libp2p::mdns;
use libp2p::multiaddr::Protocol;
use libp2p::request_response::{self, Message, OutboundRequestId};
use libp2p::swarm::SwarmEvent;
use libp2p::{noise, tcp, yamux, Multiaddr, PeerId, Swarm};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, warn};

const COMMAND_CHANNEL_CAPACITY: usize = 256;
const SWARM_IDLE_TIMEOUT_SECS: u64 = 60;

#[derive(Debug, Clone)]
pub struct NodeStatusSnapshot {
    pub peer_id: PeerId,
    pub listen_addrs: Vec<Multiaddr>,
    pub connected_peers: usize,
    pub known_peers: usize,
    pub mdns_enabled: bool,
    pub announcements_enabled: bool,
    pub local_file_count: usize,
    pub providing_count: usize,
    pub active_downloads: usize,
}

#[derive(Debug, Clone)]
pub struct PeerSnapshot {
    pub peer_id: PeerId,
    pub addresses: Vec<Multiaddr>,
    pub connected: bool,
    pub dialing: bool,
}

#[derive(Clone)]
pub struct NodeClient {
    command_tx: mpsc::Sender<NodeCommand>,
}

pub struct NodeHandle {
    client: NodeClient,
    shutdown_tx: watch::Sender<bool>,
}

impl NodeHandle {
    pub fn client(&self) -> NodeClient {
        self.client.clone()
    }

    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    pub fn shutdown(&self) -> Result<()> {
        self.shutdown_tx
            .send(true)
            .map_err(|error| anyhow!("failed to broadcast shutdown: {error}"))
    }
}

impl NodeClient {
    pub fn new(command_tx: mpsc::Sender<NodeCommand>) -> Self {
        Self { command_tx }
    }

    pub async fn add_file(&self, path: PathBuf, public: bool) -> Result<String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(NodeCommand::AddFile {
                path,
                public,
                reply: reply_tx,
            })
            .await
            .map_err(|error| anyhow!("failed to send AddFile command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("AddFile response dropped: {error}"))?
    }

    pub async fn provide(&self, cid: String) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(NodeCommand::Provide {
                cid,
                reply: reply_tx,
            })
            .await
            .map_err(|error| anyhow!("failed to send Provide command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("Provide response dropped: {error}"))?
    }

    pub async fn list_local(&self) -> Result<Vec<String>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(NodeCommand::ListLocal { reply: reply_tx })
            .await
            .map_err(|error| anyhow!("failed to send ListLocal command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("ListLocal response dropped: {error}"))?
    }

    pub async fn list_providing(&self) -> Result<Vec<String>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(NodeCommand::ListProviding { reply: reply_tx })
            .await
            .map_err(|error| anyhow!("failed to send ListProviding command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("ListProviding response dropped: {error}"))?
    }

    pub async fn status(&self) -> Result<NodeStatusSnapshot> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(NodeCommand::Status { reply: reply_tx })
            .await
            .map_err(|error| anyhow!("failed to send Status command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("Status response dropped: {error}"))?
    }

    pub async fn peers(&self) -> Result<Vec<PeerSnapshot>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(NodeCommand::Peers { reply: reply_tx })
            .await
            .map_err(|error| anyhow!("failed to send Peers command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("Peers response dropped: {error}"))?
    }

    pub async fn start_download(&self, cid: String, output_path: PathBuf) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(NodeCommand::StartDownload {
                cid,
                output_path,
                reply: reply_tx,
            })
            .await
            .map_err(|error| anyhow!("failed to send StartDownload command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("StartDownload response dropped: {error}"))?
    }

    pub async fn download_status(&self, cid: String) -> Result<Option<DownloadProgress>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(NodeCommand::DownloadStatus {
                cid,
                reply: reply_tx,
            })
            .await
            .map_err(|error| anyhow!("failed to send DownloadStatus command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("DownloadStatus response dropped: {error}"))?
    }

    pub async fn cancel_download(&self, cid: String) -> Result<bool> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(NodeCommand::CancelDownload {
                cid,
                reply: reply_tx,
            })
            .await
            .map_err(|error| anyhow!("failed to send CancelDownload command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("CancelDownload response dropped: {error}"))?
    }

    pub async fn find_providers(&self, cid: String) -> Result<Vec<PeerId>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(NodeCommand::FindProviders {
                cid,
                reply: reply_tx,
            })
            .await
            .map_err(|error| anyhow!("failed to send FindProviders command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("FindProviders response dropped: {error}"))?
    }

    pub async fn fetch_manifest(&self, peer: PeerId, cid: String) -> Result<Manifest> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(NodeCommand::FetchManifest {
                peer,
                cid,
                reply: reply_tx,
            })
            .await
            .map_err(|error| anyhow!("failed to send FetchManifest command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("FetchManifest response dropped: {error}"))?
    }

    pub async fn fetch_chunk(&self, peer: PeerId, chunk_hash: String) -> Result<Bytes> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(NodeCommand::FetchChunk {
                peer,
                chunk_hash,
                reply: reply_tx,
            })
            .await
            .map_err(|error| anyhow!("failed to send FetchChunk command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("FetchChunk response dropped: {error}"))?
    }
}

pub enum NodeCommand {
    AddFile {
        path: PathBuf,
        public: bool,
        reply: oneshot::Sender<Result<String>>,
    },
    ListLocal {
        reply: oneshot::Sender<Result<Vec<String>>>,
    },
    Provide {
        cid: String,
        reply: oneshot::Sender<Result<()>>,
    },
    ListProviding {
        reply: oneshot::Sender<Result<Vec<String>>>,
    },
    Status {
        reply: oneshot::Sender<Result<NodeStatusSnapshot>>,
    },
    Peers {
        reply: oneshot::Sender<Result<Vec<PeerSnapshot>>>,
    },
    FindProviders {
        cid: String,
        reply: oneshot::Sender<Result<Vec<PeerId>>>,
    },
    FetchManifest {
        peer: PeerId,
        cid: String,
        reply: oneshot::Sender<Result<Manifest>>,
    },
    FetchChunk {
        peer: PeerId,
        chunk_hash: String,
        reply: oneshot::Sender<Result<Bytes>>,
    },
    StartDownload {
        cid: String,
        output_path: PathBuf,
        reply: oneshot::Sender<Result<()>>,
    },
    DownloadStatus {
        cid: String,
        reply: oneshot::Sender<Result<Option<DownloadProgress>>>,
    },
    CancelDownload {
        cid: String,
        reply: oneshot::Sender<Result<bool>>,
    },
    DownloadFinished {
        cid: String,
    },
}

pub async fn start_node(
    args: DaemonArgs,
) -> Result<(NodeHandle, tokio::task::JoinHandle<Result<()>>)> {
    let identity = load_or_create_identity(&args.key_file)
        .await
        .with_context(|| format!("failed to load identity from {}", args.key_file.display()))?;

    let store = RocksStore::open(RocksStorageConfig {
        path: args.db_path.clone(),
        ..RocksStorageConfig::default()
    })
    .await
    .with_context(|| format!("failed to open RocksDB at {}", args.db_path.display()))?;

    let mut swarm = build_swarm(&identity, &args)?;
    swarm
        .listen_on(args.listen_p2p.clone())
        .with_context(|| format!("failed to listen on {}", args.listen_p2p))?;

    let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let runtime = NodeRuntime::new(args, store, swarm, command_rx, command_tx.clone());
    let runtime_task = tokio::spawn(async move { runtime.run(shutdown_rx).await });

    Ok((
        NodeHandle {
            client: NodeClient::new(command_tx),
            shutdown_tx,
        },
        runtime_task,
    ))
}

fn build_swarm(
    identity: &libp2p::identity::Keypair,
    args: &DaemonArgs,
) -> Result<Swarm<NodeBehaviour>> {
    let behaviour_args = args.clone();

    let swarm = libp2p::SwarmBuilder::with_existing_identity(identity.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(move |keypair| {
            NodeBehaviour::new(keypair, &behaviour_args)
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
        })?
        .with_swarm_config(|config| {
            config.with_idle_connection_timeout(Duration::from_secs(SWARM_IDLE_TIMEOUT_SECS))
        })
        .build();

    Ok(swarm)
}

struct NodeRuntime {
    args: DaemonArgs,
    swarm: Swarm<NodeBehaviour>,
    store: RocksStore,
    command_rx: mpsc::Receiver<NodeCommand>,
    command_tx: mpsc::Sender<NodeCommand>,
    local_peer_id: PeerId,
    peers: PeerState,
    pending_metadata: HashMap<OutboundRequestId, oneshot::Sender<Result<Manifest>>>,
    pending_chunk: HashMap<OutboundRequestId, oneshot::Sender<Result<Bytes>>>,
    pending_find_providers: HashMap<QueryId, PendingFindProviders>,
    pending_provide: HashMap<QueryId, PendingProvide>,
    active_downloads: HashMap<String, watch::Sender<bool>>,
}

struct PendingFindProviders {
    sender: oneshot::Sender<Result<Vec<PeerId>>>,
    providers: HashSet<PeerId>,
}

struct PendingProvide {
    cid: String,
    sender: Option<oneshot::Sender<Result<()>>>,
}

impl NodeRuntime {
    fn new(
        args: DaemonArgs,
        store: RocksStore,
        swarm: Swarm<NodeBehaviour>,
        command_rx: mpsc::Receiver<NodeCommand>,
        command_tx: mpsc::Sender<NodeCommand>,
    ) -> Self {
        let local_peer_id = *swarm.local_peer_id();
        let mut runtime = Self {
            peers: PeerState::new(local_peer_id, args.dial_cooldown()),
            args,
            swarm,
            store,
            command_rx,
            command_tx,
            local_peer_id,
            pending_metadata: HashMap::new(),
            pending_chunk: HashMap::new(),
            pending_find_providers: HashMap::new(),
            pending_provide: HashMap::new(),
            active_downloads: HashMap::new(),
        };

        let configured_peers = runtime.args.peers.clone();
        for address in configured_peers {
            runtime.dial_configured_peer(address);
        }

        runtime
    }

    async fn run(mut self, mut shutdown_rx: watch::Receiver<bool>) -> Result<()> {
        info!("node runtime started: peer_id={}", self.local_peer_id);

        let mut reprovide_timer = interval(self.args.reprovide_interval());
        reprovide_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    if let Err(error) = self.handle_swarm_event(event).await {
                        error!("swarm event handling error: {error:#}");
                    }
                }
                maybe_command = self.command_rx.recv() => {
                    match maybe_command {
                        Some(command) => {
                            if let Err(error) = self.handle_command(command).await {
                                error!("command handling error: {error:#}");
                            }
                        }
                        None => {
                            info!("command channel closed, shutting down node runtime");
                            break;
                        }
                    }
                }
                _ = reprovide_timer.tick() => {
                    if let Err(error) = self.handle_reprovide_tick().await {
                        warn!("reprovide tick failed: {error:#}");
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        info!("shutdown signal received, stopping node runtime");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<NodeBehaviourEvent>) -> Result<()> {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                self.peers.listen_addrs.insert(address.clone());
                info!("listening on {address}");
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                let remote_address = endpoint.get_remote_address().clone();
                self.register_peer_address(peer_id, remote_address.clone());
                self.peers.on_connected(peer_id, remote_address);

                if let Err(error) = self.swarm.behaviour_mut().kademlia.bootstrap() {
                    debug!("kademlia bootstrap skipped: {error}");
                }
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => {
                if num_established == 0 {
                    self.peers.on_disconnected(&peer_id);
                }
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if let Some(peer_id) = peer_id {
                    self.peers.on_dial_finished(&peer_id);
                }
                debug!("outgoing connection error: {error}");
            }
            SwarmEvent::Behaviour(event) => {
                self.handle_behaviour_event(event).await?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn handle_behaviour_event(&mut self, event: NodeBehaviourEvent) -> Result<()> {
        match event {
            NodeBehaviourEvent::Ping(_) => {}
            NodeBehaviourEvent::Identify(event) => {
                if let libp2p::identify::Event::Received { peer_id, info, .. } = *event {
                    for address in info.listen_addrs {
                        self.register_peer_address(peer_id, address);
                    }
                }
            }
            NodeBehaviourEvent::Mdns(event) => {
                self.handle_mdns_event(event);
            }
            NodeBehaviourEvent::Kademlia(event) => {
                self.handle_kademlia_event(*event).await?;
            }
            NodeBehaviourEvent::Metadata(event) => {
                self.handle_metadata_event(*event).await?;
            }
            NodeBehaviourEvent::Chunk(event) => {
                self.handle_chunk_event(*event).await?;
            }
            NodeBehaviourEvent::Gossipsub(event) => {
                self.handle_gossipsub_event(*event).await?;
            }
        }

        Ok(())
    }

    fn handle_mdns_event(&mut self, event: mdns::Event) {
        match event {
            mdns::Event::Discovered(peers) => {
                for (peer_id, address) in peers {
                    self.register_peer_address(peer_id, address.clone());
                    self.dial_known_peer(peer_id, address);
                }
            }
            mdns::Event::Expired(peers) => {
                for (peer_id, address) in peers {
                    self.peers.remove_discovered_address(&peer_id, &address);
                }
            }
        }
    }

    async fn handle_kademlia_event(&mut self, event: kad::Event) -> Result<()> {
        if let kad::Event::OutboundQueryProgressed {
            id, result, step, ..
        } = event
        {
            match result {
                QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
                    providers,
                    ..
                })) => {
                    if let Some(pending) = self.pending_find_providers.get_mut(&id) {
                        pending.providers.extend(providers);
                        if step.last {
                            let providers: Vec<PeerId> =
                                pending.providers.iter().copied().collect();
                            if let Some(pending) = self.pending_find_providers.remove(&id) {
                                let _ = pending.sender.send(Ok(providers));
                            }
                        }
                    }
                }
                QueryResult::GetProviders(Ok(
                    kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. },
                )) => {
                    if let Some(pending) = self.pending_find_providers.remove(&id) {
                        let providers: Vec<PeerId> = pending.providers.iter().copied().collect();
                        let _ = pending.sender.send(Ok(providers));
                    }
                }
                QueryResult::GetProviders(Err(error)) => {
                    if let Some(pending) = self.pending_find_providers.remove(&id) {
                        let _ = pending
                            .sender
                            .send(Err(anyhow!("provider lookup failed: {error}")));
                    }
                }
                QueryResult::StartProviding(Ok(_)) | QueryResult::RepublishProvider(Ok(_)) => {
                    if let Some(pending) = self.pending_provide.remove(&id) {
                        let state = ProvideState {
                            cid: pending.cid.clone(),
                            last_provided_unix_ms: now_unix_ms(),
                        };

                        let write_result = self.store.put_provide_state(&state).await;
                        if let Some(sender) = pending.sender {
                            let _ = match write_result {
                                Ok(()) => sender.send(Ok(())),
                                Err(error) => sender
                                    .send(Err(error.context("failed to persist provide state"))),
                            };
                        }
                    }
                }
                QueryResult::StartProviding(Err(error))
                | QueryResult::RepublishProvider(Err(error)) => {
                    if let Some(pending) = self.pending_provide.remove(&id) {
                        if let Some(sender) = pending.sender {
                            let _ = sender.send(Err(anyhow!("start_providing failed: {error}")));
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn handle_metadata_event(
        &mut self,
        event: request_response::Event<MetadataRequest, MetadataResponse>,
    ) -> Result<()> {
        match event {
            request_response::Event::Message { message, .. } => match message {
                Message::Request {
                    request, channel, ..
                } => {
                    let response = match self.store.get_manifest(&request.cid).await? {
                        Some(manifest) => MetadataResponse::Found { manifest },
                        None => MetadataResponse::NotFound {
                            error: "manifest not found".to_string(),
                        },
                    };

                    if let Err(error) = self
                        .swarm
                        .behaviour_mut()
                        .metadata
                        .send_response(channel, response)
                    {
                        warn!("failed to send metadata response: {error:?}");
                    }
                }
                Message::Response {
                    request_id,
                    response,
                } => {
                    if let Some(sender) = self.pending_metadata.remove(&request_id) {
                        let _ = match response {
                            MetadataResponse::Found { manifest } => sender.send(Ok(manifest)),
                            MetadataResponse::NotFound { error } => {
                                sender.send(Err(anyhow!("metadata not found: {error}")))
                            }
                        };
                    }
                }
            },
            request_response::Event::OutboundFailure {
                request_id, error, ..
            } => {
                if let Some(sender) = self.pending_metadata.remove(&request_id) {
                    let _ = sender.send(Err(anyhow!("metadata request failed: {error}")));
                }
            }
            request_response::Event::InboundFailure { error, .. } => {
                debug!("metadata inbound failure: {error}");
            }
            request_response::Event::ResponseSent { .. } => {}
        }

        Ok(())
    }

    async fn handle_chunk_event(
        &mut self,
        event: request_response::Event<ChunkRequest, ChunkResponse>,
    ) -> Result<()> {
        match event {
            request_response::Event::Message { message, .. } => match message {
                Message::Request {
                    request, channel, ..
                } => {
                    let response = match self.store.get_chunk(&request.chunk_hash).await? {
                        Some(bytes) => chunk_response_from_bytes(bytes),
                        None => ChunkResponse::NotFound {
                            error: "chunk not found".to_string(),
                        },
                    };

                    if let Err(error) = self
                        .swarm
                        .behaviour_mut()
                        .chunk
                        .send_response(channel, response)
                    {
                        warn!("failed to send chunk response: {error:?}");
                    }
                }
                Message::Response {
                    request_id,
                    response,
                } => {
                    if let Some(sender) = self.pending_chunk.remove(&request_id) {
                        let _ = match chunk_bytes(&response) {
                            Some(bytes) => sender.send(Ok(bytes)),
                            None => sender.send(Err(anyhow!("chunk not found"))),
                        };
                    }
                }
            },
            request_response::Event::OutboundFailure {
                request_id, error, ..
            } => {
                if let Some(sender) = self.pending_chunk.remove(&request_id) {
                    let _ = sender.send(Err(anyhow!("chunk request failed: {error}")));
                }
            }
            request_response::Event::InboundFailure { error, .. } => {
                debug!("chunk inbound failure: {error}");
            }
            request_response::Event::ResponseSent { .. } => {}
        }

        Ok(())
    }

    async fn handle_gossipsub_event(&mut self, event: gossipsub::Event) -> Result<()> {
        if let gossipsub::Event::Message {
            propagation_source,
            message,
            ..
        } = event
        {
            if message.data.len() > self.args.metadata_max_bytes {
                warn!(
                    "dropping oversized announcement from {}",
                    propagation_source
                );
                return Ok(());
            }

            let announcement: PublicAnnouncement = match bincode::deserialize(&message.data) {
                Ok(announcement) => announcement,
                Err(error) => {
                    warn!("dropping malformed announcement: {error}");
                    return Ok(());
                }
            };

            if let Err(error) = announcement.validate() {
                warn!("dropping invalid announcement: {error}");
                return Ok(());
            }

            info!(
                "public announcement from {}: cid={} file_len={} chunk_size={}",
                propagation_source,
                announcement.cid,
                announcement.file_len,
                announcement.chunk_size
            );
        }

        Ok(())
    }

    async fn handle_command(&mut self, command: NodeCommand) -> Result<()> {
        match command {
            NodeCommand::AddFile {
                path,
                public,
                reply,
            } => {
                let result = self.handle_add_file(path, public).await;
                let _ = reply.send(result);
            }
            NodeCommand::ListLocal { reply } => {
                let result = self.store.list_local().await;
                let _ = reply.send(result);
            }
            NodeCommand::Provide { cid, reply } => {
                let result = self.begin_provide(cid, Some(reply));
                if let Err(error) = result {
                    warn!("provide command failed before query start: {error:#}");
                }
            }
            NodeCommand::ListProviding { reply } => {
                let result = self
                    .store
                    .list_providing()
                    .await
                    .map(|items| items.into_iter().map(|item| item.cid).collect());
                let _ = reply.send(result);
            }
            NodeCommand::Status { reply } => {
                let result = self.build_status_snapshot().await;
                let _ = reply.send(result);
            }
            NodeCommand::Peers { reply } => {
                let _ = reply.send(Ok(self.peers.snapshots()));
            }
            NodeCommand::FindProviders { cid, reply } => {
                validate_cid_hex(&cid)?;
                let key = NodeBehaviour::local_record_key(&cid);
                let query_id = self.swarm.behaviour_mut().kademlia.get_providers(key);
                self.pending_find_providers.insert(
                    query_id,
                    PendingFindProviders {
                        sender: reply,
                        providers: HashSet::new(),
                    },
                );
            }
            NodeCommand::FetchManifest { peer, cid, reply } => {
                validate_cid_hex(&cid)?;
                let request_id = self
                    .swarm
                    .behaviour_mut()
                    .metadata
                    .send_request(&peer, MetadataRequest { cid });
                self.pending_metadata.insert(request_id, reply);
            }
            NodeCommand::FetchChunk {
                peer,
                chunk_hash,
                reply,
            } => {
                validate_chunk_hash_hex(&chunk_hash)?;
                let request_id = self
                    .swarm
                    .behaviour_mut()
                    .chunk
                    .send_request(&peer, ChunkRequest { chunk_hash });
                self.pending_chunk.insert(request_id, reply);
            }
            NodeCommand::StartDownload {
                cid,
                output_path,
                reply,
            } => {
                let result = self.start_download(cid, output_path).await;
                let _ = reply.send(result);
            }
            NodeCommand::DownloadStatus { cid, reply } => {
                let result = self.store.get_download_progress(&cid).await;
                let _ = reply.send(result);
            }
            NodeCommand::CancelDownload { cid, reply } => {
                let result = self.cancel_download(cid).await;
                let _ = reply.send(result);
            }
            NodeCommand::DownloadFinished { cid } => {
                self.active_downloads.remove(&cid);
            }
        }

        Ok(())
    }

    async fn handle_add_file(&mut self, path: PathBuf, public: bool) -> Result<String> {
        let cid = self
            .store
            .add_file(&path, self.args.chunk_size)
            .await
            .with_context(|| format!("failed to add file {}", path.display()))?;

        if public {
            self.publish_announcement(&cid).await?;
        }

        Ok(cid)
    }

    async fn publish_announcement(&mut self, cid: &str) -> Result<()> {
        if !self.swarm.behaviour().is_public_announcements_enabled() {
            debug!("public announcement requested but gossipsub is disabled");
            return Ok(());
        }

        let manifest = self
            .store
            .get_manifest(cid)
            .await?
            .ok_or_else(|| anyhow!("manifest missing for cid {cid}"))?;

        let announcement = PublicAnnouncement {
            cid: cid.to_string(),
            file_len: manifest.file_len,
            chunk_size: manifest.chunk_size,
        };
        announcement.validate()?;

        let payload = bincode::serialize(&announcement)?;
        if payload.len() > self.args.metadata_max_bytes {
            return Err(anyhow!(
                "announcement size {} exceeds limit {}",
                payload.len(),
                self.args.metadata_max_bytes
            ));
        }

        let topic = gossipsub::IdentTopic::new(self.args.public_topic.clone());
        if let Some(gossipsub) = self.swarm.behaviour_mut().gossipsub.as_mut() {
            gossipsub
                .publish(topic, payload)
                .map_err(|error| anyhow!("failed to publish announcement: {error}"))?;
        }

        Ok(())
    }

    async fn build_status_snapshot(&mut self) -> Result<NodeStatusSnapshot> {
        let local_file_count = self.store.list_local().await?.len();
        let providing_count = self.store.list_providing().await?.len();

        Ok(NodeStatusSnapshot {
            peer_id: self.local_peer_id,
            listen_addrs: self.peers.listen_addrs.iter().cloned().collect(),
            connected_peers: self.peers.connected_peer_count(),
            known_peers: self.peers.known_peer_count(),
            mdns_enabled: self.swarm.behaviour().is_mdns_enabled(),
            announcements_enabled: self.swarm.behaviour().is_public_announcements_enabled(),
            local_file_count,
            providing_count,
            active_downloads: self.active_downloads.len(),
        })
    }

    fn begin_provide(
        &mut self,
        cid: String,
        sender: Option<oneshot::Sender<Result<()>>>,
    ) -> Result<()> {
        validate_cid_hex(&cid)?;
        let key = NodeBehaviour::local_record_key(&cid);

        let query_id = self
            .swarm
            .behaviour_mut()
            .kademlia
            .start_providing(key)
            .map_err(|error| anyhow!("failed to start_providing: {error}"))?;

        self.pending_provide
            .insert(query_id, PendingProvide { cid, sender });

        Ok(())
    }

    async fn start_download(&mut self, cid: String, output_path: PathBuf) -> Result<()> {
        validate_cid_hex(&cid)?;

        if self.active_downloads.contains_key(&cid) {
            return Err(anyhow!("download already active for cid {cid}"));
        }

        let (cancel_tx, cancel_rx) = watch::channel(false);
        self.active_downloads.insert(cid.clone(), cancel_tx);

        let client = NodeClient::new(self.command_tx.clone());
        let store = self.store.clone();
        let command_tx = self.command_tx.clone();
        let download_config = DownloadConfig {
            global_concurrency: self.args.global_download_concurrency.max(1),
            per_peer_concurrency: self.args.per_peer_concurrency.max(1),
            max_retries: 4,
        };
        let cid_for_task = cid.clone();

        tokio::spawn(async move {
            if let Err(error) = run_download(
                client,
                store,
                download_config,
                cid_for_task.clone(),
                output_path,
                cancel_rx,
            )
            .await
            {
                warn!("download task failed for cid {}: {error:#}", cid_for_task);
            }

            let _ = command_tx
                .send(NodeCommand::DownloadFinished { cid: cid_for_task })
                .await;
        });

        Ok(())
    }

    async fn cancel_download(&mut self, cid: String) -> Result<bool> {
        let mut cancelled = false;

        if let Some(cancel_tx) = self.active_downloads.get(&cid) {
            let _ = cancel_tx.send(true);
            cancelled = true;
        }

        if let Some(mut progress) = self.store.get_download_progress(&cid).await? {
            if !matches!(
                progress.phase,
                DownloadPhase::Completed | DownloadPhase::Failed
            ) {
                progress.phase = DownloadPhase::Cancelled;
                progress.error = Some("cancelled".to_string());
                progress.updated_unix_ms = now_unix_ms();
                self.store.put_download_progress(&progress).await?;
                cancelled = true;
            }
        }

        Ok(cancelled)
    }

    async fn handle_reprovide_tick(&mut self) -> Result<()> {
        let entries = self.store.list_providing().await?;

        for entry in entries {
            if let Err(error) = self.begin_provide(entry.cid.clone(), None) {
                warn!("reprovide skipped for {}: {error:#}", entry.cid);
            }
        }

        Ok(())
    }

    fn dial_configured_peer(&mut self, address: Multiaddr) {
        let peer_id = peer_id_from_multiaddr(&address);
        if let Some(peer_id) = peer_id {
            self.register_peer_address(peer_id, address.clone());
            self.dial_known_peer(peer_id, address);
            return;
        }

        if let Err(error) = self.swarm.dial(address.clone()) {
            warn!("failed to dial configured address {address}: {error}");
        }
    }

    fn register_peer_address(&mut self, peer_id: PeerId, address: Multiaddr) {
        if peer_id == self.local_peer_id {
            return;
        }

        self.swarm.add_peer_address(peer_id, address.clone());
        self.swarm
            .behaviour_mut()
            .kademlia
            .add_address(&peer_id, address.clone());
        self.peers.add_discovered(peer_id, address);
    }

    fn dial_known_peer(&mut self, peer_id: PeerId, address: Multiaddr) {
        if !self.peers.can_dial(&peer_id) || self.swarm.is_connected(&peer_id) {
            return;
        }

        self.peers.on_dial_started(peer_id);
        if let Err(error) = self.swarm.dial(address.clone()) {
            self.peers.on_dial_finished(&peer_id);
            warn!("failed to dial peer {} at {}: {}", peer_id, address, error);
        }
    }
}

#[derive(Default)]
struct PeerState {
    connected: HashMap<PeerId, HashSet<Multiaddr>>,
    discovered: HashMap<PeerId, HashSet<Multiaddr>>,
    dialing: HashSet<PeerId>,
    last_dial_attempt: HashMap<PeerId, Instant>,
    listen_addrs: HashSet<Multiaddr>,
    local_peer_id: Option<PeerId>,
    dial_cooldown: Option<Duration>,
}

impl PeerState {
    fn new(local_peer_id: PeerId, dial_cooldown: Duration) -> Self {
        Self {
            local_peer_id: Some(local_peer_id),
            dial_cooldown: Some(dial_cooldown),
            ..Self::default()
        }
    }

    fn add_discovered(&mut self, peer_id: PeerId, address: Multiaddr) {
        if self.connected.contains_key(&peer_id) {
            self.connected.entry(peer_id).or_default().insert(address);
            return;
        }
        self.discovered.entry(peer_id).or_default().insert(address);
    }

    fn remove_discovered_address(&mut self, peer_id: &PeerId, address: &Multiaddr) {
        if let Some(addresses) = self.discovered.get_mut(peer_id) {
            addresses.remove(address);
            if addresses.is_empty() {
                self.discovered.remove(peer_id);
            }
        }
    }

    fn on_connected(&mut self, peer_id: PeerId, address: Multiaddr) {
        self.connected.entry(peer_id).or_default().insert(address);
        self.dialing.remove(&peer_id);
        self.discovered.remove(&peer_id);
    }

    fn on_disconnected(&mut self, peer_id: &PeerId) {
        self.connected.remove(peer_id);
        self.dialing.remove(peer_id);
    }

    fn can_dial(&self, peer_id: &PeerId) -> bool {
        if Some(*peer_id) == self.local_peer_id {
            return false;
        }

        if self.connected.contains_key(peer_id) || self.dialing.contains(peer_id) {
            return false;
        }

        if let Some(last_attempt) = self.last_dial_attempt.get(peer_id) {
            if let Some(cooldown) = self.dial_cooldown {
                if last_attempt.elapsed() < cooldown {
                    return false;
                }
            }
        }

        true
    }

    fn on_dial_started(&mut self, peer_id: PeerId) {
        self.dialing.insert(peer_id);
        self.last_dial_attempt.insert(peer_id, Instant::now());
    }

    fn on_dial_finished(&mut self, peer_id: &PeerId) {
        self.dialing.remove(peer_id);
    }

    fn connected_peer_count(&self) -> usize {
        self.connected.len()
    }

    fn known_peer_count(&self) -> usize {
        let mut peers = HashSet::new();
        peers.extend(self.connected.keys().copied());
        peers.extend(self.discovered.keys().copied());
        peers.extend(self.dialing.iter().copied());
        peers.len()
    }

    fn snapshots(&self) -> Vec<PeerSnapshot> {
        let mut peer_ids = HashSet::new();
        peer_ids.extend(self.connected.keys().copied());
        peer_ids.extend(self.discovered.keys().copied());
        peer_ids.extend(self.dialing.iter().copied());

        let mut snapshots = Vec::with_capacity(peer_ids.len());
        for peer_id in peer_ids {
            let mut addresses = HashSet::new();
            if let Some(known) = self.connected.get(&peer_id) {
                addresses.extend(known.iter().cloned());
            }
            if let Some(known) = self.discovered.get(&peer_id) {
                addresses.extend(known.iter().cloned());
            }

            snapshots.push(PeerSnapshot {
                peer_id,
                addresses: addresses.into_iter().collect(),
                connected: self.connected.contains_key(&peer_id),
                dialing: self.dialing.contains(&peer_id),
            });
        }

        snapshots.sort_by_key(|snapshot| snapshot.peer_id.to_string());
        snapshots
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PublicAnnouncement {
    cid: String,
    file_len: u64,
    chunk_size: u32,
}

impl PublicAnnouncement {
    fn validate(&self) -> Result<()> {
        validate_cid_hex(&self.cid)?;
        if self.chunk_size == 0 {
            return Err(anyhow!("announcement chunk_size must be non-zero"));
        }
        Ok(())
    }
}

fn peer_id_from_multiaddr(address: &Multiaddr) -> Option<PeerId> {
    address.iter().find_map(|protocol| match protocol {
        Protocol::P2p(peer_id) => Some(peer_id),
        _ => None,
    })
}
