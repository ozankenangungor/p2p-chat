use super::client::{NodeClient, NodeCommand};
use super::peer_state::{peer_id_from_multiaddr, PeerState};
use super::NodeStatusSnapshot;
use crate::behaviour::{NodeBehaviour, NodeBehaviourEvent};
use crate::config::DaemonArgs;
use crate::download::{run_download, DownloadConfig};
use crate::manifest::Manifest;
use crate::protocol::{
    chunk_bytes, chunk_response_from_bytes, ChunkRequest, ChunkResponse, MetadataRequest,
    MetadataResponse,
};
use crate::storage::{now_unix_ms, DownloadPhase, ProvideState, RocksStore};
use crate::validation::{validate_chunk_hash_hex, validate_cid_hex};
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use libp2p::futures::StreamExt;
use libp2p::gossipsub;
use libp2p::kad::{self, QueryId, QueryResult};
use libp2p::mdns;
use libp2p::request_response::{self, Message, OutboundRequestId};
use libp2p::swarm::SwarmEvent;
use libp2p::{Multiaddr, PeerId, Swarm};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, warn};

pub(super) struct NodeRuntime {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PublicAnnouncement {
    cid: String,
    file_len: u64,
    chunk_size: u32,
}

impl NodeRuntime {
    pub(super) fn new(
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

    pub(super) async fn run(mut self, mut shutdown_rx: watch::Receiver<bool>) -> Result<()> {
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
                let result = self.begin_provide(cid, Some(reply)).await;
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

    async fn begin_provide(
        &mut self,
        cid: String,
        sender: Option<oneshot::Sender<Result<()>>>,
    ) -> Result<()> {
        let start_result: Result<QueryId> = async {
            validate_cid_hex(&cid)?;
            if self.store.get_manifest(&cid).await?.is_none() {
                return Err(anyhow!("local manifest not found for cid {cid}"));
            }

            let key = NodeBehaviour::local_record_key(&cid);
            self.swarm
                .behaviour_mut()
                .kademlia
                .start_providing(key)
                .map_err(|error| anyhow!("failed to start_providing: {error}"))
        }
        .await;

        match start_result {
            Ok(query_id) => {
                self.pending_provide
                    .insert(query_id, PendingProvide { cid, sender });
                Ok(())
            }
            Err(error) => {
                if let Some(sender) = sender {
                    let _ = sender.send(Err(error));
                    return Ok(());
                }

                Err(error)
            }
        }
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
            provider_discovery_retries: 8,
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
            if self.store.get_manifest(&entry.cid).await?.is_none() {
                warn!(
                    "dropping stale provide state for missing local cid {}",
                    entry.cid
                );
                self.store.remove_provide_state(&entry.cid).await?;
                continue;
            }

            if let Err(error) = self.begin_provide(entry.cid.clone(), None).await {
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

impl PublicAnnouncement {
    fn validate(&self) -> Result<()> {
        validate_cid_hex(&self.cid)?;
        if self.chunk_size == 0 {
            return Err(anyhow!("announcement chunk_size must be non-zero"));
        }
        Ok(())
    }
}
