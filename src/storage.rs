use crate::manifest::Manifest;
use crate::merkle::{compute_merkle_root_hex, hash_bytes_hex};
use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBCompressionType, IteratorMode, Options,
    WriteOptions, DB,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::AsyncReadExt;

pub const CF_META: &str = "meta";
pub const CF_CHUNK: &str = "chunk";
pub const CF_PROVIDE: &str = "provide";
pub const CF_DOWNLOAD: &str = "download";

#[derive(Debug, Clone)]
pub struct RocksStorageConfig {
    pub path: PathBuf,
    pub wal_fsync: bool,
    pub write_buffer_size: usize,
    pub max_write_buffers: i32,
}

impl Default for RocksStorageConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("./data/rocksdb"),
            wal_fsync: false,
            write_buffer_size: 64 * 1024 * 1024,
            max_write_buffers: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProvideState {
    pub cid: String,
    pub last_provided_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DownloadPhase {
    Queued,
    DiscoveringProviders,
    FetchingManifest,
    DownloadingChunks,
    Assembling,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DownloadProgress {
    pub cid: String,
    pub output_path: PathBuf,
    pub phase: DownloadPhase,
    pub total_chunks: u32,
    pub completed_chunks: Vec<bool>,
    pub retries: u32,
    pub error: Option<String>,
    pub updated_unix_ms: u64,
}

impl DownloadProgress {
    pub fn new(cid: String, output_path: PathBuf) -> Self {
        Self {
            cid,
            output_path,
            phase: DownloadPhase::Queued,
            total_chunks: 0,
            completed_chunks: Vec::new(),
            retries: 0,
            error: None,
            updated_unix_ms: now_unix_ms(),
        }
    }

    pub fn with_chunk_plan(mut self, total_chunks: usize) -> Result<Self> {
        self.total_chunks = u32::try_from(total_chunks)?;
        self.completed_chunks = vec![false; total_chunks];
        self.updated_unix_ms = now_unix_ms();
        Ok(self)
    }
}

#[derive(Clone)]
pub struct RocksStore {
    db: Arc<DB>,
    wal_fsync: bool,
}

impl RocksStore {
    pub async fn open(config: RocksStorageConfig) -> Result<Self> {
        let wal_fsync = config.wal_fsync;
        let db = tokio::task::spawn_blocking(move || open_db(config.clone()))
            .await
            .map_err(|e| anyhow!("failed to join RocksDB open task: {e}"))??;

        Ok(Self {
            db: Arc::new(db),
            wal_fsync,
        })
    }

    pub async fn add_file(&self, path: &Path, chunk_size: usize) -> Result<String> {
        if chunk_size == 0 {
            bail!("chunk_size must be greater than zero");
        }

        let chunk_size_u32 = u32::try_from(chunk_size)?;
        let mut file = tokio::fs::File::open(path).await?;
        let file_len = file.metadata().await?.len();

        let mut buf = vec![0_u8; chunk_size];
        let mut chunk_hashes = Vec::new();

        loop {
            let read = file.read(&mut buf).await?;
            if read == 0 {
                break;
            }

            let chunk = Bytes::copy_from_slice(&buf[..read]);
            let chunk_hash = hash_bytes_hex(&chunk);
            self.put_chunk(&chunk_hash, chunk).await?;
            chunk_hashes.push(chunk_hash);
        }

        let cid = compute_merkle_root_hex(&chunk_hashes)?;
        let original_filename = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string());
        let manifest = Manifest::new(
            file_len,
            chunk_size_u32,
            chunk_hashes,
            cid.clone(),
            original_filename,
        );

        self.put_manifest(&cid, &manifest).await?;
        Ok(cid)
    }

    pub async fn put_manifest(&self, cid: &str, manifest: &Manifest) -> Result<()> {
        let key = cid.as_bytes().to_vec();
        let value = serde_json::to_vec(manifest)?;
        let wal_fsync = self.wal_fsync;
        self.run_db(move |db| {
            let cf = cf_handle(&db, CF_META)?;
            let mut write_options = WriteOptions::default();
            write_options.set_sync(wal_fsync);
            db.put_cf_opt(&cf, key, value, &write_options)?;
            Ok(())
        })
        .await
    }

    pub async fn get_manifest(&self, cid: &str) -> Result<Option<Manifest>> {
        let key = cid.as_bytes().to_vec();
        self.run_db(move |db| {
            let cf = cf_handle(&db, CF_META)?;
            let value = db.get_cf(&cf, key)?;
            value
                .as_deref()
                .map(serde_json::from_slice::<Manifest>)
                .transpose()
                .map_err(Into::into)
        })
        .await
    }

    pub async fn put_chunk(&self, chunk_hash: &str, chunk: Bytes) -> Result<()> {
        let key = chunk_hash.as_bytes().to_vec();
        let value = chunk.to_vec();
        let wal_fsync = self.wal_fsync;
        self.run_db(move |db| {
            let cf = cf_handle(&db, CF_CHUNK)?;
            let mut write_options = WriteOptions::default();
            write_options.set_sync(wal_fsync);
            db.put_cf_opt(&cf, key, value, &write_options)?;
            Ok(())
        })
        .await
    }

    pub async fn get_chunk(&self, chunk_hash: &str) -> Result<Option<Bytes>> {
        let key = chunk_hash.as_bytes().to_vec();
        self.run_db(move |db| {
            let cf = cf_handle(&db, CF_CHUNK)?;
            let value = db.get_pinned_cf(&cf, key)?;
            Ok(value.map(|v| Bytes::copy_from_slice(v.as_ref())))
        })
        .await
    }

    pub async fn list_local(&self) -> Result<Vec<String>> {
        self.run_db(move |db| {
            let cf = cf_handle(&db, CF_META)?;
            let iter = db.iterator_cf(&cf, IteratorMode::Start);
            let mut cids = Vec::new();
            for entry in iter {
                let (key, _) = entry?;
                cids.push(String::from_utf8_lossy(&key).to_string());
            }
            Ok(cids)
        })
        .await
    }

    pub async fn count_local(&self) -> Result<usize> {
        self.count_entries(CF_META).await
    }

    pub async fn put_provide_state(&self, state: &ProvideState) -> Result<()> {
        let key = state.cid.as_bytes().to_vec();
        let value = serde_json::to_vec(state)?;
        let wal_fsync = self.wal_fsync;
        self.run_db(move |db| {
            let cf = cf_handle(&db, CF_PROVIDE)?;
            let mut write_options = WriteOptions::default();
            write_options.set_sync(wal_fsync);
            db.put_cf_opt(&cf, key, value, &write_options)?;
            Ok(())
        })
        .await
    }

    pub async fn remove_provide_state(&self, cid: &str) -> Result<()> {
        let key = cid.as_bytes().to_vec();
        let wal_fsync = self.wal_fsync;
        self.run_db(move |db| {
            let cf = cf_handle(&db, CF_PROVIDE)?;
            let mut write_options = WriteOptions::default();
            write_options.set_sync(wal_fsync);
            db.delete_cf_opt(&cf, key, &write_options)?;
            Ok(())
        })
        .await
    }

    pub async fn list_providing(&self) -> Result<Vec<ProvideState>> {
        self.run_db(move |db| {
            let cf = cf_handle(&db, CF_PROVIDE)?;
            let iter = db.iterator_cf(&cf, IteratorMode::Start);
            let mut items = Vec::new();
            for entry in iter {
                let (_, value) = entry?;
                let item: ProvideState = serde_json::from_slice(&value)?;
                items.push(item);
            }
            Ok(items)
        })
        .await
    }

    pub async fn count_providing(&self) -> Result<usize> {
        self.count_entries(CF_PROVIDE).await
    }

    pub async fn put_download_progress(&self, progress: &DownloadProgress) -> Result<()> {
        let key = progress.cid.as_bytes().to_vec();
        let value = serde_json::to_vec(progress)?;
        let wal_fsync = self.wal_fsync;
        self.run_db(move |db| {
            let cf = cf_handle(&db, CF_DOWNLOAD)?;
            let mut write_options = WriteOptions::default();
            write_options.set_sync(wal_fsync);
            db.put_cf_opt(&cf, key, value, &write_options)?;
            Ok(())
        })
        .await
    }

    pub async fn get_download_progress(&self, cid: &str) -> Result<Option<DownloadProgress>> {
        let key = cid.as_bytes().to_vec();
        self.run_db(move |db| {
            let cf = cf_handle(&db, CF_DOWNLOAD)?;
            let value = db.get_cf(&cf, key)?;
            value
                .as_deref()
                .map(serde_json::from_slice::<DownloadProgress>)
                .transpose()
                .map_err(Into::into)
        })
        .await
    }

    async fn run_db<F, T>(&self, work: F) -> Result<T>
    where
        F: FnOnce(Arc<DB>) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || work(db))
            .await
            .map_err(|e| anyhow!("failed to join RocksDB task: {e}"))?
    }

    async fn count_entries(&self, cf_name: &'static str) -> Result<usize> {
        self.run_db(move |db| {
            let cf = cf_handle(&db, cf_name)?;
            let iter = db.iterator_cf(&cf, IteratorMode::Start);
            let mut count = 0_usize;
            for entry in iter {
                entry?;
                count = count.saturating_add(1);
            }
            Ok(count)
        })
        .await
    }
}

fn open_db(config: RocksStorageConfig) -> Result<DB> {
    if let Some(parent) = config.path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut db_options = Options::default();
    db_options.create_if_missing(true);
    db_options.create_missing_column_families(true);
    db_options.set_max_background_jobs(4);
    db_options.set_write_buffer_size(config.write_buffer_size);
    db_options.set_max_write_buffer_number(config.max_write_buffers);
    db_options.set_compression_type(DBCompressionType::Lz4);
    db_options.set_bottommost_compression_type(DBCompressionType::Zstd);
    db_options.set_use_fsync(config.wal_fsync);

    let mut chunk_cf_options = Options::default();
    chunk_cf_options.set_write_buffer_size(config.write_buffer_size);
    chunk_cf_options.set_max_write_buffer_number(config.max_write_buffers);
    chunk_cf_options.set_compression_type(DBCompressionType::Lz4);
    chunk_cf_options.set_bottommost_compression_type(DBCompressionType::Zstd);

    let mut default_cf_options = Options::default();
    default_cf_options.set_write_buffer_size(config.write_buffer_size / 2);
    default_cf_options.set_compression_type(DBCompressionType::Lz4);

    let cfs = vec![
        ColumnFamilyDescriptor::new(CF_META, default_cf_options.clone()),
        ColumnFamilyDescriptor::new(CF_CHUNK, chunk_cf_options),
        ColumnFamilyDescriptor::new(CF_PROVIDE, default_cf_options.clone()),
        ColumnFamilyDescriptor::new(CF_DOWNLOAD, default_cf_options),
    ];

    DB::open_cf_descriptors(&db_options, &config.path, cfs).map_err(Into::into)
}

fn cf_handle<'a>(db: &'a DB, cf_name: &str) -> Result<Arc<BoundColumnFamily<'a>>> {
    db.cf_handle(cf_name)
        .ok_or_else(|| anyhow!("missing RocksDB column family: {cf_name}"))
}

pub fn now_unix_ms() -> u64 {
    let now = SystemTime::now();
    now.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::{DownloadPhase, DownloadProgress, RocksStorageConfig, RocksStore};
    use crate::manifest::Manifest;
    use tempfile::TempDir;

    #[tokio::test]
    async fn add_file_and_manifest_roundtrip() {
        let temp_dir = TempDir::new();
        assert!(temp_dir.is_ok());
        let temp_dir = match temp_dir {
            Ok(d) => d,
            Err(_) => return,
        };

        let config = RocksStorageConfig {
            path: temp_dir.path().join("db"),
            ..RocksStorageConfig::default()
        };
        let store = RocksStore::open(config).await;
        assert!(store.is_ok());
        let store = match store {
            Ok(s) => s,
            Err(_) => return,
        };

        let input = temp_dir.path().join("sample.bin");
        let write = tokio::fs::write(&input, b"hello world from dfs test").await;
        assert!(write.is_ok());

        let cid = store.add_file(&input, 8).await;
        assert!(cid.is_ok());
        let cid = match cid {
            Ok(c) => c,
            Err(_) => return,
        };

        let manifest = store.get_manifest(&cid).await;
        assert!(manifest.is_ok());

        if let Ok(Some(manifest)) = manifest {
            assert_eq!(manifest.merkle_root, cid);
            assert!(!manifest.chunk_hashes.is_empty());
        } else {
            panic!("manifest missing");
        }
    }

    #[tokio::test]
    async fn download_progress_roundtrip() {
        let temp_dir = TempDir::new();
        assert!(temp_dir.is_ok());
        let temp_dir = match temp_dir {
            Ok(d) => d,
            Err(_) => return,
        };

        let store = RocksStore::open(RocksStorageConfig {
            path: temp_dir.path().join("db"),
            ..RocksStorageConfig::default()
        })
        .await;
        assert!(store.is_ok());
        let store = match store {
            Ok(s) => s,
            Err(_) => return,
        };

        let mut progress = DownloadProgress::new("cid-1".to_string(), temp_dir.path().join("out"));
        progress.phase = DownloadPhase::DownloadingChunks;
        let with_plan = progress.clone().with_chunk_plan(3);
        if let Ok(planned) = with_plan {
            progress = planned;
        }

        let persisted = store.put_download_progress(&progress).await;
        assert!(persisted.is_ok());

        let loaded = store.get_download_progress("cid-1").await;
        assert!(loaded.is_ok());

        if let Ok(Some(loaded)) = loaded {
            assert_eq!(loaded.cid, "cid-1");
            assert_eq!(loaded.total_chunks, 3);
        } else {
            panic!("download progress missing");
        }
    }

    #[test]
    fn manifest_type_is_serializable() {
        let manifest = Manifest::new(
            16,
            4,
            vec!["0".repeat(64)],
            "1".repeat(64),
            Some("name.bin".to_string()),
        );

        let bytes = serde_json::to_vec(&manifest);
        assert!(bytes.is_ok());
    }
}
