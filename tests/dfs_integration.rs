use anyhow::{anyhow, Result};
use p2p_chat::config::DaemonArgs;
use p2p_chat::node::start_node;
use p2p_chat::storage::DownloadPhase;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_exchange_metadata_and_chunks() -> Result<()> {
    let node_a_dir = TempDir::new()?;
    let node_b_dir = TempDir::new()?;

    let p2p_port_a = free_local_port()?;
    let p2p_port_b = free_local_port()?;
    let grpc_port_a = free_local_port()?;
    let grpc_port_b = free_local_port()?;

    let node_a_args = daemon_args(
        p2p_port_a,
        grpc_port_a,
        node_a_dir.path().to_path_buf(),
        Vec::new(),
    )?;

    let (node_a_handle, node_a_task) = start_node(node_a_args).await?;
    let node_a_status = wait_for_status(&node_a_handle).await?;

    let node_a_peer = node_a_status.peer_id;
    let node_a_addr: libp2p::Multiaddr =
        format!("/ip4/127.0.0.1/tcp/{p2p_port_a}/p2p/{node_a_peer}").parse()?;

    let node_b_args = daemon_args(
        p2p_port_b,
        grpc_port_b,
        node_b_dir.path().to_path_buf(),
        vec![node_a_addr],
    )?;

    let (node_b_handle, node_b_task) = start_node(node_b_args).await?;

    wait_for_connected_peers(&node_a_handle, 1).await?;
    wait_for_connected_peers(&node_b_handle, 1).await?;

    let source_data = b"libp2p dfs integration test payload".to_vec();
    let source_path = node_a_dir.path().join("source.bin");
    tokio::fs::write(&source_path, &source_data).await?;

    let cid = node_a_handle
        .client()
        .add_file(source_path.clone(), false)
        .await?;
    node_a_handle.client().provide(cid.clone()).await?;

    wait_for_providers(&node_b_handle, &cid).await?;

    let output_path = node_b_dir.path().join("download.bin");
    node_b_handle
        .client()
        .start_download(cid.clone(), output_path.clone())
        .await?;

    wait_for_download_completion(&node_b_handle, &cid).await?;

    let downloaded = tokio::fs::read(&output_path).await?;
    assert_eq!(downloaded, source_data);

    node_a_handle.shutdown()?;
    node_b_handle.shutdown()?;

    let node_a_result = node_a_task
        .await
        .map_err(|error| anyhow!("node A join error: {error}"))?;
    let node_b_result = node_b_task
        .await
        .map_err(|error| anyhow!("node B join error: {error}"))?;

    node_a_result?;
    node_b_result?;

    Ok(())
}

fn daemon_args(
    p2p_port: u16,
    grpc_port: u16,
    data_dir: PathBuf,
    peers: Vec<libp2p::Multiaddr>,
) -> Result<DaemonArgs> {
    Ok(DaemonArgs {
        listen_p2p: format!("/ip4/127.0.0.1/tcp/{p2p_port}").parse()?,
        grpc_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, grpc_port)),
        peers,
        mdns: false,
        key_file: data_dir.join("node_key.ed25519"),
        db_path: data_dir.join("db"),
        chunk_size: 8,
        metadata_max_bytes: 1024 * 1024,
        global_download_concurrency: 4,
        per_peer_concurrency: 2,
        dial_cooldown_secs: 1,
        reprovide_interval_secs: 5,
        enable_public_announcements: false,
        public_topic: "dfs.public.announcements".to_string(),
        log_level: "warn".to_string(),
        verbose: false,
    })
}

async fn wait_for_status(
    handle: &p2p_chat::node::NodeHandle,
) -> Result<p2p_chat::node::NodeStatusSnapshot> {
    let mut attempts = 0_u32;
    loop {
        let status = handle.client().status().await?;
        if !status.listen_addrs.is_empty() {
            return Ok(status);
        }

        attempts = attempts.saturating_add(1);
        if attempts > 20 {
            return Err(anyhow!("status listen addresses did not become available"));
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_connected_peers(
    handle: &p2p_chat::node::NodeHandle,
    expected: usize,
) -> Result<()> {
    for _ in 0..40 {
        let status = handle.client().status().await?;
        if status.connected_peers >= expected {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    Err(anyhow!("timed out waiting for connected peers"))
}

async fn wait_for_providers(handle: &p2p_chat::node::NodeHandle, cid: &str) -> Result<()> {
    for _ in 0..150 {
        let providers = handle.client().find_providers(cid.to_string()).await?;
        if !providers.is_empty() {
            return Ok(());
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    Err(anyhow!("timed out waiting for providers"))
}

async fn wait_for_download_completion(
    handle: &p2p_chat::node::NodeHandle,
    cid: &str,
) -> Result<()> {
    for _ in 0..80 {
        let maybe_progress = handle.client().download_status(cid.to_string()).await?;
        if let Some(progress) = maybe_progress {
            match progress.phase {
                DownloadPhase::Completed => return Ok(()),
                DownloadPhase::Failed => {
                    return Err(anyhow!(
                        "download failed: {}",
                        progress.error.unwrap_or_else(|| "unknown".to_string())
                    ));
                }
                DownloadPhase::Cancelled => {
                    return Err(anyhow!("download was cancelled"));
                }
                _ => {}
            }
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    Err(anyhow!("timed out waiting for download completion"))
}

fn free_local_port() -> Result<u16> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}
