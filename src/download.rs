use crate::manifest::Manifest;
use crate::merkle::{compute_merkle_root_hex, hash_bytes_hex};
use crate::node::NodeClient;
use crate::protocol::validate_manifest_cid;
use crate::storage::{now_unix_ms, DownloadPhase, DownloadProgress, RocksStore};
use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use libp2p::PeerId;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::{watch, Mutex, Semaphore};
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub struct DownloadConfig {
    pub global_concurrency: usize,
    pub per_peer_concurrency: usize,
    pub max_retries: u32,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            global_concurrency: 16,
            per_peer_concurrency: 3,
            max_retries: 4,
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("download cancelled")]
pub struct DownloadCancelled;

pub async fn run_download(
    client: NodeClient,
    store: RocksStore,
    config: DownloadConfig,
    cid: String,
    output_path: PathBuf,
    mut cancel_rx: watch::Receiver<bool>,
) -> Result<()> {
    let result = run_download_inner(
        client,
        store.clone(),
        config,
        cid.clone(),
        output_path.clone(),
        &mut cancel_rx,
    )
    .await;

    if let Err(error) = &result {
        let mut progress = match store.get_download_progress(&cid).await {
            Ok(Some(progress)) => progress,
            Ok(None) => DownloadProgress::new(cid.clone(), output_path),
            Err(_) => DownloadProgress::new(cid.clone(), output_path),
        };

        if error.downcast_ref::<DownloadCancelled>().is_some() {
            progress.phase = DownloadPhase::Cancelled;
            progress.error = Some("cancelled".to_string());
        } else {
            progress.phase = DownloadPhase::Failed;
            progress.error = Some(error.to_string());
        }
        progress.updated_unix_ms = now_unix_ms();

        let _ = store.put_download_progress(&progress).await;
    }

    result
}

async fn run_download_inner(
    client: NodeClient,
    store: RocksStore,
    config: DownloadConfig,
    cid: String,
    output_path: PathBuf,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<()> {
    ensure_not_cancelled(cancel_rx)?;

    let mut progress = store
        .get_download_progress(&cid)
        .await?
        .unwrap_or_else(|| DownloadProgress::new(cid.clone(), output_path.clone()));
    progress.output_path = output_path.clone();
    progress.phase = DownloadPhase::DiscoveringProviders;
    progress.updated_unix_ms = now_unix_ms();
    store.put_download_progress(&progress).await?;

    let providers = client.find_providers(cid.clone()).await?;
    if providers.is_empty() {
        bail!("no providers found for cid {cid}");
    }

    ensure_not_cancelled(cancel_rx)?;

    progress.phase = DownloadPhase::FetchingManifest;
    progress.updated_unix_ms = now_unix_ms();
    store.put_download_progress(&progress).await?;

    let manifest = fetch_manifest_with_retry(
        &client,
        &providers,
        &cid,
        config.max_retries,
        cancel_rx,
        &mut progress,
        &store,
    )
    .await?;
    validate_manifest_cid(&manifest, &cid)?;

    let computed_root = compute_merkle_root_hex(&manifest.chunk_hashes)?;
    if computed_root != cid {
        bail!("manifest merkle root mismatch for cid {cid}");
    }

    let chunk_count = manifest.chunk_hashes.len();
    let expected_chunk_count = u32::try_from(chunk_count)?;
    if progress.total_chunks != expected_chunk_count
        || progress.completed_chunks.len() != manifest.chunk_hashes.len()
    {
        progress =
            DownloadProgress::new(cid.clone(), output_path.clone()).with_chunk_plan(chunk_count)?;
    }

    progress.phase = DownloadPhase::DownloadingChunks;
    progress.error = None;
    progress.updated_unix_ms = now_unix_ms();
    store.put_download_progress(&progress).await?;

    download_chunks(
        &client,
        &store,
        &manifest,
        &providers,
        cancel_rx,
        &config,
        &mut progress,
    )
    .await?;

    ensure_not_cancelled(cancel_rx)?;

    progress.phase = DownloadPhase::Assembling;
    progress.updated_unix_ms = now_unix_ms();
    store.put_download_progress(&progress).await?;

    assemble_file(&store, &manifest, &cid, &output_path).await?;

    progress.phase = DownloadPhase::Completed;
    progress.error = None;
    progress.updated_unix_ms = now_unix_ms();
    store.put_download_progress(&progress).await?;

    Ok(())
}

async fn fetch_manifest_with_retry(
    client: &NodeClient,
    providers: &[PeerId],
    cid: &str,
    max_retries: u32,
    cancel_rx: &mut watch::Receiver<bool>,
    progress: &mut DownloadProgress,
    store: &RocksStore,
) -> Result<Manifest> {
    let mut last_error = anyhow!("no providers attempted");

    for retry in 0..=max_retries {
        ensure_not_cancelled(cancel_rx)?;

        for provider in providers {
            match client.fetch_manifest(*provider, cid.to_string()).await {
                Ok(manifest) => {
                    validate_manifest_cid(&manifest, cid)?;
                    return Ok(manifest);
                }
                Err(error) => {
                    last_error = error.context(format!(
                        "failed to fetch manifest from provider {}",
                        provider
                    ));
                }
            }
        }

        progress.retries = retry.saturating_add(1);
        progress.updated_unix_ms = now_unix_ms();
        store.put_download_progress(progress).await?;

        if retry < max_retries {
            tokio::time::sleep(backoff_duration(retry)).await;
        }
    }

    Err(last_error)
}

async fn download_chunks(
    client: &NodeClient,
    store: &RocksStore,
    manifest: &Manifest,
    providers: &[PeerId],
    cancel_rx: &watch::Receiver<bool>,
    config: &DownloadConfig,
    progress: &mut DownloadProgress,
) -> Result<()> {
    let global_limit = config.global_concurrency.max(1);
    let per_peer_limit = config.per_peer_concurrency.max(1);

    let global_semaphore = Arc::new(Semaphore::new(global_limit));
    let per_peer_semaphores: Arc<Mutex<HashMap<PeerId, Arc<Semaphore>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let pending_indices = pending_chunk_indices(manifest, store, progress).await?;

    let mut join_set = JoinSet::new();
    let mut cursor = 0_usize;

    while cursor < pending_indices.len() || !join_set.is_empty() {
        while cursor < pending_indices.len() && join_set.len() < global_limit {
            let index = pending_indices[cursor];
            cursor += 1;

            let chunk_hash = manifest
                .chunk_hashes
                .get(index)
                .cloned()
                .ok_or_else(|| anyhow!("chunk index {index} out of bounds"))?;

            let client_clone = client.clone();
            let providers_clone = providers.to_vec();
            let global_semaphore_clone = global_semaphore.clone();
            let per_peer_semaphores_clone = per_peer_semaphores.clone();
            let mut cancel_rx_clone = cancel_rx.clone();
            let max_retries = config.max_retries;

            join_set.spawn(async move {
                let _global_permit = global_semaphore_clone
                    .acquire_owned()
                    .await
                    .map_err(|_| anyhow!("global semaphore closed"))?;

                let bytes = fetch_chunk_with_retry(
                    &client_clone,
                    &providers_clone,
                    &chunk_hash,
                    index,
                    max_retries,
                    per_peer_limit,
                    &per_peer_semaphores_clone,
                    &mut cancel_rx_clone,
                )
                .await?;

                Ok::<(usize, String, Bytes), anyhow::Error>((index, chunk_hash, bytes))
            });
        }

        if let Some(result) = join_set.join_next().await {
            let (index, chunk_hash, bytes) =
                result.map_err(|error| anyhow!("chunk worker join failure: {error}"))??;

            if hash_bytes_hex(&bytes) != chunk_hash {
                bail!("chunk hash mismatch at index {index}");
            }

            store.put_chunk(&chunk_hash, bytes).await?;
            if let Some(flag) = progress.completed_chunks.get_mut(index) {
                *flag = true;
            }
            progress.updated_unix_ms = now_unix_ms();
            store.put_download_progress(progress).await?;
        }
    }

    Ok(())
}

async fn pending_chunk_indices(
    manifest: &Manifest,
    store: &RocksStore,
    progress: &mut DownloadProgress,
) -> Result<Vec<usize>> {
    let mut pending = Vec::new();

    for (index, hash) in manifest.chunk_hashes.iter().enumerate() {
        let already_complete = progress
            .completed_chunks
            .get(index)
            .copied()
            .unwrap_or(false);
        if already_complete {
            match store.get_chunk(hash).await? {
                Some(bytes) if hash_bytes_hex(&bytes) == *hash => {
                    continue;
                }
                _ => {
                    if let Some(flag) = progress.completed_chunks.get_mut(index) {
                        *flag = false;
                    }
                }
            }
        }

        pending.push(index);
    }

    Ok(pending)
}

async fn fetch_chunk_with_retry(
    client: &NodeClient,
    providers: &[PeerId],
    chunk_hash: &str,
    chunk_index: usize,
    max_retries: u32,
    per_peer_limit: usize,
    per_peer_semaphores: &Arc<Mutex<HashMap<PeerId, Arc<Semaphore>>>>,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<Bytes> {
    let mut last_error = anyhow!("unable to fetch chunk");

    for retry in 0..=max_retries {
        ensure_not_cancelled(cancel_rx)?;

        for offset in 0..providers.len() {
            let provider_index =
                (chunk_index + offset + usize::try_from(retry).unwrap_or(0)) % providers.len();
            let peer = providers
                .get(provider_index)
                .copied()
                .ok_or_else(|| anyhow!("missing provider"))?;

            let peer_semaphore = {
                let mut guard = per_peer_semaphores.lock().await;
                guard
                    .entry(peer)
                    .or_insert_with(|| Arc::new(Semaphore::new(per_peer_limit)))
                    .clone()
            };

            let _peer_permit = peer_semaphore
                .acquire_owned()
                .await
                .map_err(|_| anyhow!("peer semaphore closed"))?;

            match client.fetch_chunk(peer, chunk_hash.to_string()).await {
                Ok(bytes) => {
                    if hash_bytes_hex(&bytes) != chunk_hash {
                        last_error = anyhow!("received invalid chunk hash from provider {peer}");
                        continue;
                    }
                    return Ok(bytes);
                }
                Err(error) => {
                    last_error =
                        error.context(format!("failed to fetch chunk from provider {}", peer));
                }
            }
        }

        if retry < max_retries {
            tokio::time::sleep(backoff_duration(retry)).await;
        }
    }

    Err(last_error)
}

async fn assemble_file(
    store: &RocksStore,
    manifest: &Manifest,
    cid: &str,
    output_path: &PathBuf,
) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let short_cid: String = cid.chars().take(8).collect();
    let temp_path = output_path.with_extension(format!("{short_cid}.part"));
    let mut temp_file = tokio::fs::File::create(&temp_path)
        .await
        .with_context(|| format!("failed to create temporary file {}", temp_path.display()))?;

    for hash in &manifest.chunk_hashes {
        let chunk = store
            .get_chunk(hash)
            .await?
            .ok_or_else(|| anyhow!("missing chunk {hash} during assembly"))?;

        if hash_bytes_hex(&chunk) != *hash {
            bail!("chunk hash mismatch while assembling file");
        }

        temp_file.write_all(&chunk).await?;
    }

    temp_file.flush().await?;
    temp_file.sync_all().await?;

    if tokio::fs::try_exists(output_path).await? {
        tokio::fs::remove_file(output_path).await?;
    }

    tokio::fs::rename(&temp_path, output_path).await?;

    let root = compute_merkle_root_hex(&manifest.chunk_hashes)?;
    if root != cid {
        bail!("assembled file failed merkle root verification");
    }

    Ok(())
}

fn ensure_not_cancelled(cancel_rx: &watch::Receiver<bool>) -> Result<()> {
    if *cancel_rx.borrow() {
        bail!(DownloadCancelled);
    }

    Ok(())
}

fn backoff_duration(retry: u32) -> Duration {
    let capped_retry = retry.min(8);
    let multiplier = 1_u64 << capped_retry;
    Duration::from_millis(150_u64.saturating_mul(multiplier))
}
