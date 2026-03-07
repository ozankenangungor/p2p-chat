use libp2p::{Multiaddr, PeerId};

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
