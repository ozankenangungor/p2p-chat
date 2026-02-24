use crate::config::DaemonArgs;
use crate::protocol::{
    ChunkCodec, ChunkRequest, ChunkResponse, MetadataCodec, MetadataRequest, MetadataResponse,
    ProtocolLimits, CHUNK_PROTOCOL, METADATA_PROTOCOL,
};
use libp2p::gossipsub;
use libp2p::identify;
use libp2p::kad::{self, store::MemoryStore};
use libp2p::mdns;
use libp2p::ping;
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::swarm::{behaviour::toggle::Toggle, NetworkBehaviour};
use libp2p::{identity::Keypair, StreamProtocol};
use std::io;
use std::time::Duration;

const REQUEST_TIMEOUT_SECS: u64 = 20;
const MAX_CONCURRENT_STREAMS: usize = 128;

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "NodeBehaviourEvent")]
pub struct NodeBehaviour {
    pub ping: ping::Behaviour,
    pub identify: identify::Behaviour,
    pub mdns: Toggle<mdns::tokio::Behaviour>,
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub metadata: request_response::Behaviour<MetadataCodec>,
    pub chunk: request_response::Behaviour<ChunkCodec>,
    pub gossipsub: Toggle<gossipsub::Behaviour>,
}

#[derive(Debug)]
pub enum NodeBehaviourEvent {
    Ping(ping::Event),
    Identify(Box<identify::Event>),
    Mdns(mdns::Event),
    Kademlia(Box<kad::Event>),
    Metadata(Box<request_response::Event<MetadataRequest, MetadataResponse>>),
    Chunk(Box<request_response::Event<ChunkRequest, ChunkResponse>>),
    Gossipsub(Box<gossipsub::Event>),
}

impl From<ping::Event> for NodeBehaviourEvent {
    fn from(event: ping::Event) -> Self {
        Self::Ping(event)
    }
}

impl From<identify::Event> for NodeBehaviourEvent {
    fn from(event: identify::Event) -> Self {
        Self::Identify(Box::new(event))
    }
}

impl From<mdns::Event> for NodeBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        Self::Mdns(event)
    }
}

impl From<kad::Event> for NodeBehaviourEvent {
    fn from(event: kad::Event) -> Self {
        Self::Kademlia(Box::new(event))
    }
}

impl From<request_response::Event<MetadataRequest, MetadataResponse>> for NodeBehaviourEvent {
    fn from(event: request_response::Event<MetadataRequest, MetadataResponse>) -> Self {
        Self::Metadata(Box::new(event))
    }
}

impl From<request_response::Event<ChunkRequest, ChunkResponse>> for NodeBehaviourEvent {
    fn from(event: request_response::Event<ChunkRequest, ChunkResponse>) -> Self {
        Self::Chunk(Box::new(event))
    }
}

impl From<gossipsub::Event> for NodeBehaviourEvent {
    fn from(event: gossipsub::Event) -> Self {
        Self::Gossipsub(Box::new(event))
    }
}

impl NodeBehaviour {
    pub fn new(local_key: &Keypair, args: &DaemonArgs) -> io::Result<Self> {
        let local_peer_id = local_key.public().to_peer_id();

        let ping = ping::Behaviour::new(ping::Config::new().with_interval(Duration::from_secs(10)));
        let identify = identify::Behaviour::new(identify::Config::new(
            "/dfs/1.0.0".to_string(),
            local_key.public(),
        ));

        let mdns = if args.mdns {
            let config = mdns::Config::default();
            Toggle::from(Some(mdns::tokio::Behaviour::new(config, local_peer_id)?))
        } else {
            Toggle::from(None)
        };

        let mut kad_config = kad::Config::new(StreamProtocol::new("/ipfs/kad/1.0.0"));
        kad_config.set_query_timeout(Duration::from_secs(30));
        let store = MemoryStore::new(local_peer_id);
        let mut kademlia = kad::Behaviour::with_config(local_peer_id, store, kad_config);
        kademlia.set_mode(Some(kad::Mode::Server));

        let limits = ProtocolLimits {
            max_request_bytes: 8 * 1024,
            max_metadata_response_bytes: args.metadata_max_bytes,
            max_chunk_response_bytes: args.chunk_response_limit(),
        };

        let rr_config = request_response::Config::default()
            .with_request_timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .with_max_concurrent_streams(MAX_CONCURRENT_STREAMS);

        let metadata = request_response::Behaviour::with_codec(
            MetadataCodec::new(limits),
            [(
                StreamProtocol::new(METADATA_PROTOCOL),
                ProtocolSupport::Full,
            )],
            rr_config.clone(),
        );

        let chunk = request_response::Behaviour::with_codec(
            ChunkCodec::new(limits),
            [(StreamProtocol::new(CHUNK_PROTOCOL), ProtocolSupport::Full)],
            rr_config,
        );

        let gossipsub = if args.enable_public_announcements {
            let mut config_builder = gossipsub::ConfigBuilder::default();
            config_builder.max_transmit_size(args.metadata_max_bytes);
            config_builder.validation_mode(gossipsub::ValidationMode::Strict);

            let config = config_builder
                .build()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
            let mut behaviour = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(local_key.clone()),
                config,
            )
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

            let topic = gossipsub::IdentTopic::new(args.public_topic.clone());
            let _ = behaviour.subscribe(&topic);
            Toggle::from(Some(behaviour))
        } else {
            Toggle::from(None)
        };

        Ok(Self {
            ping,
            identify,
            mdns,
            kademlia,
            metadata,
            chunk,
            gossipsub,
        })
    }

    pub fn is_mdns_enabled(&self) -> bool {
        self.mdns.is_enabled()
    }

    pub fn is_public_announcements_enabled(&self) -> bool {
        self.gossipsub.is_enabled()
    }

    pub fn local_record_key(cid: &str) -> kad::RecordKey {
        kad::RecordKey::new(&cid)
    }
}

pub fn swarm_protocols() -> [&'static str; 2] {
    [METADATA_PROTOCOL, CHUNK_PROTOCOL]
}

#[cfg(test)]
mod tests {
    use super::NodeBehaviour;
    use crate::config::{DaemonArgs, DEFAULT_METADATA_MAX_BYTES};
    use libp2p::identity::Keypair;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::path::PathBuf;

    fn daemon_args(mdns: bool) -> DaemonArgs {
        let listen_p2p: libp2p::Multiaddr = "/ip4/127.0.0.1/tcp/0"
            .parse()
            .unwrap_or_else(|_| libp2p::Multiaddr::empty());

        DaemonArgs {
            listen_p2p,
            grpc_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 50051)),
            allow_remote_control: false,
            peers: Vec::new(),
            mdns,
            key_file: PathBuf::from("./node_key"),
            db_path: PathBuf::from("./db"),
            chunk_size: 256 * 1024,
            metadata_max_bytes: DEFAULT_METADATA_MAX_BYTES,
            global_download_concurrency: 16,
            per_peer_concurrency: 3,
            dial_cooldown_secs: 15,
            reprovide_interval_secs: 300,
            enable_public_announcements: false,
            public_topic: "dfs.public.announcements".to_string(),
            log_level: "info".to_string(),
            verbose: false,
        }
    }

    #[test]
    fn mdns_disabled_toggles_behaviour_off() {
        let args = daemon_args(false);
        let key = Keypair::generate_ed25519();
        let behaviour = NodeBehaviour::new(&key, &args);
        assert!(behaviour.is_ok());
        if let Ok(behaviour) = behaviour {
            assert!(!behaviour.is_mdns_enabled());
        }
    }
}
