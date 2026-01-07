use clap::{ArgAction, Args, Parser, Subcommand};
use libp2p::Multiaddr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;
pub const DEFAULT_METADATA_MAX_BYTES: usize = 1024 * 1024;
pub const DEFAULT_GLOBAL_DOWNLOAD_CONCURRENCY: usize = 16;
pub const DEFAULT_PER_PEER_CONCURRENCY: usize = 3;
pub const DEFAULT_DIAL_COOLDOWN_SECS: u64 = 15;
pub const DEFAULT_REPROVIDE_INTERVAL_SECS: u64 = 300;
pub const DEFAULT_GRPC_ADDR: &str = "127.0.0.1:50051";
pub const DEFAULT_GRPC_ENDPOINT: &str = "http://127.0.0.1:50051";
pub const DEFAULT_LISTEN_P2P: &str = "/ip4/0.0.0.0/tcp/0";
pub const DEFAULT_PUBLIC_TOPIC: &str = "dfs.public.announcements";

#[derive(Parser, Debug, Clone)]
#[command(
    name = "p2p-chat",
    author,
    version,
    about = "Production-grade libp2p DFS node",
    long_about = None
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    Daemon(DaemonArgs),
    Add(ClientAddArgs),
    Provide(ClientProvideArgs),
    Get(ClientGetArgs),
    List(ClientBaseArgs),
    Status(ClientBaseArgs),
    DownloadStatus(ClientDownloadStatusArgs),
    CancelDownload(ClientCancelDownloadArgs),
    Peers(ClientBaseArgs),
}

#[derive(Args, Debug, Clone)]
pub struct DaemonArgs {
    #[arg(long, env = "DFS_LISTEN_P2P", default_value = DEFAULT_LISTEN_P2P)]
    pub listen_p2p: Multiaddr,

    #[arg(long, env = "DFS_GRPC_ADDR", default_value = DEFAULT_GRPC_ADDR)]
    pub grpc_addr: SocketAddr,

    #[arg(long = "peer", env = "DFS_PEERS")]
    pub peers: Vec<Multiaddr>,

    #[arg(long, env = "DFS_MDNS", default_value_t = true, action = ArgAction::Set)]
    pub mdns: bool,

    #[arg(long, env = "DFS_KEY_FILE", default_value = "./data/node_key.ed25519")]
    pub key_file: PathBuf,

    #[arg(long, env = "DFS_DB_PATH", default_value = "./data/rocksdb")]
    pub db_path: PathBuf,

    #[arg(long, env = "DFS_CHUNK_SIZE", default_value_t = DEFAULT_CHUNK_SIZE)]
    pub chunk_size: usize,

    #[arg(
        long,
        env = "DFS_METADATA_MAX_BYTES",
        default_value_t = DEFAULT_METADATA_MAX_BYTES
    )]
    pub metadata_max_bytes: usize,

    #[arg(
        long,
        env = "DFS_GLOBAL_DOWNLOAD_CONCURRENCY",
        default_value_t = DEFAULT_GLOBAL_DOWNLOAD_CONCURRENCY
    )]
    pub global_download_concurrency: usize,

    #[arg(
        long,
        env = "DFS_PER_PEER_CONCURRENCY",
        default_value_t = DEFAULT_PER_PEER_CONCURRENCY
    )]
    pub per_peer_concurrency: usize,

    #[arg(
        long,
        env = "DFS_DIAL_COOLDOWN_SECS",
        default_value_t = DEFAULT_DIAL_COOLDOWN_SECS
    )]
    pub dial_cooldown_secs: u64,

    #[arg(
        long,
        env = "DFS_REPROVIDE_INTERVAL_SECS",
        default_value_t = DEFAULT_REPROVIDE_INTERVAL_SECS
    )]
    pub reprovide_interval_secs: u64,

    #[arg(
        long,
        env = "DFS_ENABLE_PUBLIC_ANNOUNCEMENTS",
        default_value_t = false,
        action = ArgAction::Set
    )]
    pub enable_public_announcements: bool,

    #[arg(long, env = "DFS_PUBLIC_TOPIC", default_value = DEFAULT_PUBLIC_TOPIC)]
    pub public_topic: String,

    #[arg(long, env = "DFS_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    #[arg(short, long, env = "DFS_VERBOSE", default_value_t = false)]
    pub verbose: bool,
}

impl DaemonArgs {
    pub fn dial_cooldown(&self) -> Duration {
        Duration::from_secs(self.dial_cooldown_secs)
    }

    pub fn reprovide_interval(&self) -> Duration {
        Duration::from_secs(self.reprovide_interval_secs)
    }

    pub fn log_filter(&self) -> String {
        if self.verbose {
            "debug".to_string()
        } else {
            self.log_level.clone()
        }
    }

    pub fn chunk_response_limit(&self) -> usize {
        self.chunk_size.saturating_add(1024)
    }
}

#[derive(Args, Debug, Clone)]
pub struct ClientBaseArgs {
    #[arg(long, env = "DFS_GRPC_ENDPOINT", default_value = DEFAULT_GRPC_ENDPOINT)]
    pub grpc_addr: String,
}

#[derive(Args, Debug, Clone)]
pub struct ClientAddArgs {
    pub path: PathBuf,

    #[arg(long, default_value_t = false)]
    pub public: bool,

    #[command(flatten)]
    pub base: ClientBaseArgs,
}

#[derive(Args, Debug, Clone)]
pub struct ClientProvideArgs {
    pub cid: String,

    #[command(flatten)]
    pub base: ClientBaseArgs,
}

#[derive(Args, Debug, Clone)]
pub struct ClientGetArgs {
    pub cid: String,

    #[arg(short = 'o', long)]
    pub output: PathBuf,

    #[command(flatten)]
    pub base: ClientBaseArgs,
}

#[derive(Args, Debug, Clone)]
pub struct ClientDownloadStatusArgs {
    pub cid: String,

    #[command(flatten)]
    pub base: ClientBaseArgs,
}

#[derive(Args, Debug, Clone)]
pub struct ClientCancelDownloadArgs {
    pub cid: String,

    #[command(flatten)]
    pub base: ClientBaseArgs,
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}

#[cfg(test)]
mod tests {
    use super::{DaemonArgs, DEFAULT_CHUNK_SIZE, DEFAULT_METADATA_MAX_BYTES};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::path::PathBuf;

    #[test]
    fn daemon_chunk_response_limit_tracks_chunk_size() {
        let listen_p2p = "/ip4/0.0.0.0/tcp/0".parse();
        assert!(listen_p2p.is_ok());
        let listen_p2p = match listen_p2p {
            Ok(value) => value,
            Err(_) => return,
        };

        let args = DaemonArgs {
            listen_p2p,
            grpc_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 50051)),
            peers: Vec::new(),
            mdns: false,
            key_file: PathBuf::from("./node_key"),
            db_path: PathBuf::from("./db"),
            chunk_size: 1024,
            metadata_max_bytes: DEFAULT_METADATA_MAX_BYTES,
            global_download_concurrency: 16,
            per_peer_concurrency: 3,
            dial_cooldown_secs: 10,
            reprovide_interval_secs: 30,
            enable_public_announcements: false,
            public_topic: "dfs.public.announcements".to_string(),
            log_level: "info".to_string(),
            verbose: false,
        };

        assert_eq!(args.chunk_response_limit(), 2048);
    }

    #[test]
    fn default_chunk_size_constant_is_expected() {
        assert_eq!(DEFAULT_CHUNK_SIZE, 256 * 1024);
    }
}
