use super::{NodeStatusSnapshot, PeerSnapshot};
use crate::manifest::Manifest;
use crate::storage::DownloadProgress;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use libp2p::PeerId;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone)]
pub struct NodeClient {
    command_tx: mpsc::Sender<NodeCommand>,
}

pub struct NodeHandle {
    pub(super) client: NodeClient,
    pub(super) shutdown_tx: watch::Sender<bool>,
}

pub enum NodeCommand {
    AddFile {
        path: PathBuf,
        public: bool,
        reply: oneshot::Sender<Result<String>>,
    },
    ListLocal {
        reply: oneshot::Sender<Result<Vec<String>>>,
    },
    Provide {
        cid: String,
        reply: oneshot::Sender<Result<()>>,
    },
    ListProviding {
        reply: oneshot::Sender<Result<Vec<String>>>,
    },
    Status {
        reply: oneshot::Sender<Result<NodeStatusSnapshot>>,
    },
    Peers {
        reply: oneshot::Sender<Result<Vec<PeerSnapshot>>>,
    },
    FindProviders {
        cid: String,
        reply: oneshot::Sender<Result<Vec<PeerId>>>,
    },
    FetchManifest {
        peer: PeerId,
        cid: String,
        reply: oneshot::Sender<Result<Manifest>>,
    },
    FetchChunk {
        peer: PeerId,
        chunk_hash: String,
        reply: oneshot::Sender<Result<Bytes>>,
    },
    StartDownload {
        cid: String,
        output_path: PathBuf,
        reply: oneshot::Sender<Result<()>>,
    },
    DownloadStatus {
        cid: String,
        reply: oneshot::Sender<Result<Option<DownloadProgress>>>,
    },
    CancelDownload {
        cid: String,
        reply: oneshot::Sender<Result<bool>>,
    },
    DownloadFinished {
        cid: String,
    },
}

impl NodeHandle {
    pub fn client(&self) -> NodeClient {
        self.client.clone()
    }

    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    pub fn shutdown(&self) -> Result<()> {
        self.shutdown_tx
            .send(true)
            .map_err(|error| anyhow!("failed to broadcast shutdown: {error}"))
    }
}

impl NodeClient {
    pub fn new(command_tx: mpsc::Sender<NodeCommand>) -> Self {
        Self { command_tx }
    }

    pub async fn add_file(&self, path: PathBuf, public: bool) -> Result<String> {
        self.request(
            |reply| NodeCommand::AddFile {
                path,
                public,
                reply,
            },
            "AddFile",
        )
        .await
    }

    pub async fn provide(&self, cid: String) -> Result<()> {
        self.request(|reply| NodeCommand::Provide { cid, reply }, "Provide")
            .await
    }

    pub async fn list_local(&self) -> Result<Vec<String>> {
        self.request(|reply| NodeCommand::ListLocal { reply }, "ListLocal")
            .await
    }

    pub async fn list_providing(&self) -> Result<Vec<String>> {
        self.request(
            |reply| NodeCommand::ListProviding { reply },
            "ListProviding",
        )
        .await
    }

    pub async fn status(&self) -> Result<NodeStatusSnapshot> {
        self.request(|reply| NodeCommand::Status { reply }, "Status")
            .await
    }

    pub async fn peers(&self) -> Result<Vec<PeerSnapshot>> {
        self.request(|reply| NodeCommand::Peers { reply }, "Peers")
            .await
    }

    pub async fn start_download(&self, cid: String, output_path: PathBuf) -> Result<()> {
        self.request(
            |reply| NodeCommand::StartDownload {
                cid,
                output_path,
                reply,
            },
            "StartDownload",
        )
        .await
    }

    pub async fn download_status(&self, cid: String) -> Result<Option<DownloadProgress>> {
        self.request(
            |reply| NodeCommand::DownloadStatus { cid, reply },
            "DownloadStatus",
        )
        .await
    }

    pub async fn cancel_download(&self, cid: String) -> Result<bool> {
        self.request(
            |reply| NodeCommand::CancelDownload { cid, reply },
            "CancelDownload",
        )
        .await
    }

    pub async fn find_providers(&self, cid: String) -> Result<Vec<PeerId>> {
        self.request(
            |reply| NodeCommand::FindProviders { cid, reply },
            "FindProviders",
        )
        .await
    }

    pub async fn fetch_manifest(&self, peer: PeerId, cid: String) -> Result<Manifest> {
        self.request(
            |reply| NodeCommand::FetchManifest { peer, cid, reply },
            "FetchManifest",
        )
        .await
    }

    pub async fn fetch_chunk(&self, peer: PeerId, chunk_hash: String) -> Result<Bytes> {
        self.request(
            |reply| NodeCommand::FetchChunk {
                peer,
                chunk_hash,
                reply,
            },
            "FetchChunk",
        )
        .await
    }

    async fn request<T, F>(&self, build: F, command_name: &str) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(oneshot::Sender<Result<T>>) -> NodeCommand,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(build(reply_tx))
            .await
            .map_err(|error| anyhow!("failed to send {command_name} command: {error}"))?;
        reply_rx
            .await
            .map_err(|error| anyhow!("{command_name} response dropped: {error}"))?
    }
}
