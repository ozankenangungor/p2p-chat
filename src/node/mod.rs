mod client;
mod peer_state;
mod runtime;
mod types;

use crate::behaviour::NodeBehaviour;
use crate::config::DaemonArgs;
use crate::identity::load_or_create_identity;
use crate::storage::{RocksStorageConfig, RocksStore};
use anyhow::{Context, Result};
use libp2p::{noise, tcp, yamux, Swarm};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

pub use client::{NodeClient, NodeCommand, NodeHandle};
pub use types::{NodeStatusSnapshot, PeerSnapshot};

use runtime::NodeRuntime;

const COMMAND_CHANNEL_CAPACITY: usize = 256;
const SWARM_IDLE_TIMEOUT_SECS: u64 = 60;

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
