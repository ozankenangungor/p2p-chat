use anyhow::{anyhow, Result};
use libp2p::identity::Keypair;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

pub async fn load_or_create_identity(path: &Path) -> Result<Keypair> {
    match tokio::fs::read(path).await {
        Ok(bytes) => decode_keypair(&bytes),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            create_identity_file(path).await?;
            let bytes = tokio::fs::read(path).await?;
            decode_keypair(&bytes)
        }
        Err(error) => Err(error.into()),
    }
}

fn decode_keypair(bytes: &[u8]) -> Result<Keypair> {
    Keypair::from_protobuf_encoding(bytes)
        .map_err(|e| anyhow!("invalid key file protobuf encoding: {e}"))
}

async fn create_identity_file(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let bytes = Keypair::generate_ed25519()
        .to_protobuf_encoding()
        .map_err(|e| anyhow!("failed to encode generated keypair: {e}"))?;
    write_private_file(path.to_path_buf(), bytes).await
}

async fn write_private_file(path: PathBuf, bytes: Vec<u8>) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        #[cfg(unix)]
        {
            use std::fs::OpenOptions;
            use std::os::unix::fs::OpenOptionsExt;

            let mut options = OpenOptions::new();
            options.write(true).create_new(true).mode(0o600);
            let mut file = options.open(&path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            Ok::<(), io::Error>(())
        }

        #[cfg(not(unix))]
        {
            use std::fs::OpenOptions;

            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            let mut file = options.open(&path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            Ok::<(), io::Error>(())
        }
    })
    .await
    .map_err(|e| anyhow!("identity file write task failed: {e}"))?
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::load_or_create_identity;
    use tempfile::TempDir;

    #[tokio::test]
    async fn key_file_is_deterministic_after_creation() {
        let dir = TempDir::new();
        assert!(dir.is_ok());
        let dir = match dir {
            Ok(dir) => dir,
            Err(_) => return,
        };

        let path = dir.path().join("node_key.ed25519");
        let k1 = load_or_create_identity(&path).await;
        assert!(k1.is_ok());
        let k2 = load_or_create_identity(&path).await;
        assert!(k2.is_ok());

        if let (Ok(k1), Ok(k2)) = (k1, k2) {
            assert_eq!(k1.public().to_peer_id(), k2.public().to_peer_id());
        }
    }
}
