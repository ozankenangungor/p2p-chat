use super::PeerSnapshot;
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct PeerState {
    pub(super) connected: HashMap<PeerId, HashSet<Multiaddr>>,
    pub(super) discovered: HashMap<PeerId, HashSet<Multiaddr>>,
    pub(super) dialing: HashSet<PeerId>,
    pub(super) last_dial_attempt: HashMap<PeerId, Instant>,
    pub(super) listen_addrs: HashSet<Multiaddr>,
    pub(super) local_peer_id: Option<PeerId>,
    pub(super) dial_cooldown: Option<Duration>,
}

impl PeerState {
    pub(super) fn new(local_peer_id: PeerId, dial_cooldown: Duration) -> Self {
        Self {
            local_peer_id: Some(local_peer_id),
            dial_cooldown: Some(dial_cooldown),
            ..Self::default()
        }
    }

    pub(super) fn add_discovered(&mut self, peer_id: PeerId, address: Multiaddr) {
        if self.connected.contains_key(&peer_id) {
            self.connected.entry(peer_id).or_default().insert(address);
            return;
        }
        self.discovered.entry(peer_id).or_default().insert(address);
    }

    pub(super) fn remove_discovered_address(&mut self, peer_id: &PeerId, address: &Multiaddr) {
        if let Some(addresses) = self.discovered.get_mut(peer_id) {
            addresses.remove(address);
            if addresses.is_empty() {
                self.discovered.remove(peer_id);
            }
        }
    }

    pub(super) fn on_connected(&mut self, peer_id: PeerId, address: Multiaddr) {
        self.connected.entry(peer_id).or_default().insert(address);
        self.dialing.remove(&peer_id);
        self.discovered.remove(&peer_id);
    }

    pub(super) fn on_disconnected(&mut self, peer_id: &PeerId) {
        self.connected.remove(peer_id);
        self.dialing.remove(peer_id);
    }

    pub(super) fn can_dial(&self, peer_id: &PeerId) -> bool {
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

    pub(super) fn on_dial_started(&mut self, peer_id: PeerId) {
        self.dialing.insert(peer_id);
        self.last_dial_attempt.insert(peer_id, Instant::now());
    }

    pub(super) fn on_dial_finished(&mut self, peer_id: &PeerId) {
        self.dialing.remove(peer_id);
    }

    pub(super) fn connected_peer_count(&self) -> usize {
        self.connected.len()
    }

    pub(super) fn known_peer_count(&self) -> usize {
        let mut peers = HashSet::new();
        peers.extend(self.connected.keys().copied());
        peers.extend(self.discovered.keys().copied());
        peers.extend(self.dialing.iter().copied());
        peers.len()
    }

    pub(super) fn snapshots(&self) -> Vec<PeerSnapshot> {
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

pub(super) fn peer_id_from_multiaddr(address: &Multiaddr) -> Option<PeerId> {
    address.iter().find_map(|protocol| match protocol {
        Protocol::P2p(peer_id) => Some(peer_id),
        _ => None,
    })
}
