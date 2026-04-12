use crate::grpc_api::dfs::control::v1::dfs_control_server::DfsControl;
use crate::grpc_api::dfs::control::v1::{
    AddFileRequest, AddFileResponse, CancelDownloadRequest, CancelDownloadResponse,
    DownloadStatusRequest, DownloadStatusResponse, Empty, GetFileRequest, GetFileResponse,
    ListLocalResponse, ListProvidingResponse, PeerEntry, PeersResponse, ProvideRequest,
    ProvideResponse, StatusResponse,
};
use crate::node::{NodeClient, PeerSnapshot};
use crate::validation::validate_cid_hex;
use anyhow::Result;
use std::path::PathBuf;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct DfsControlService {
    client: NodeClient,
}

impl DfsControlService {
    pub fn new(client: NodeClient) -> Self {
        Self { client }
    }
}

#[tonic::async_trait]
impl DfsControl for DfsControlService {
    async fn add_file(
        &self,
        request: Request<AddFileRequest>,
    ) -> Result<Response<AddFileResponse>, Status> {
        let request = request.into_inner();
        let path = parse_path(&request.path, "path")?;

        let cid = self
            .client
            .add_file(path, request.public)
            .await
            .map_err(to_status)?;

        Ok(Response::new(AddFileResponse { cid }))
    }

    async fn provide(
        &self,
        request: Request<ProvideRequest>,
    ) -> Result<Response<ProvideResponse>, Status> {
        let request = request.into_inner();
        let cid = validate_cid(request.cid)?;
        self.client.provide(cid).await.map_err(to_status)?;

        Ok(Response::new(ProvideResponse { accepted: true }))
    }

    async fn get_file(
        &self,
        request: Request<GetFileRequest>,
    ) -> Result<Response<GetFileResponse>, Status> {
        let request = request.into_inner();
        let cid = validate_cid(request.cid)?;
        let output_path = parse_path(&request.output_path, "output_path")?;

        self.client
            .start_download(cid, output_path)
            .await
            .map_err(to_status)?;

        Ok(Response::new(GetFileResponse { accepted: true }))
    }

    async fn list_local(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ListLocalResponse>, Status> {
        let cids = self.client.list_local().await.map_err(to_status)?;
        Ok(Response::new(ListLocalResponse { cids }))
    }

    async fn list_providing(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ListProvidingResponse>, Status> {
        let cids = self.client.list_providing().await.map_err(to_status)?;
        Ok(Response::new(ListProvidingResponse { cids }))
    }

    async fn download_status(
        &self,
        request: Request<DownloadStatusRequest>,
    ) -> Result<Response<DownloadStatusResponse>, Status> {
        let request = request.into_inner();
        let cid = validate_cid(request.cid)?;
        let progress = self.client.download_status(cid).await.map_err(to_status)?;

        let progress = progress.ok_or_else(|| Status::not_found("download not found"))?;
        let completed_chunks = u32::try_from(
            progress
                .completed_chunks
                .iter()
                .filter(|done| **done)
                .count(),
        )
        .map_err(|_| Status::internal("completed chunk count overflow"))?;

        Ok(Response::new(DownloadStatusResponse {
            cid: progress.cid,
            phase: format!("{:?}", progress.phase),
            total_chunks: progress.total_chunks,
            completed_chunks,
            output_path: progress.output_path.display().to_string(),
            error: progress.error.unwrap_or_default(),
        }))
    }

    async fn cancel_download(
        &self,
        request: Request<CancelDownloadRequest>,
    ) -> Result<Response<CancelDownloadResponse>, Status> {
        let request = request.into_inner();
        let cid = validate_cid(request.cid)?;
        let cancelled = self.client.cancel_download(cid).await.map_err(to_status)?;

        Ok(Response::new(CancelDownloadResponse { cancelled }))
    }

    async fn peers(&self, _request: Request<Empty>) -> Result<Response<PeersResponse>, Status> {
        let peers = self.client.peers().await.map_err(to_status)?;
        Ok(Response::new(PeersResponse {
            peers: peers.into_iter().map(peer_entry).collect(),
        }))
    }

    async fn status(&self, _request: Request<Empty>) -> Result<Response<StatusResponse>, Status> {
        let snapshot = self.client.status().await.map_err(to_status)?;

        Ok(Response::new(StatusResponse {
            peer_id: snapshot.peer_id.to_string(),
            listen_addrs: snapshot
                .listen_addrs
                .iter()
                .map(ToString::to_string)
                .collect(),
            connected_peers: u32::try_from(snapshot.connected_peers)
                .map_err(|_| Status::internal("connected peer count overflow"))?,
            known_peers: u32::try_from(snapshot.known_peers)
                .map_err(|_| Status::internal("known peer count overflow"))?,
            mdns_enabled: snapshot.mdns_enabled,
            announcements_enabled: snapshot.announcements_enabled,
            local_file_count: u32::try_from(snapshot.local_file_count)
                .map_err(|_| Status::internal("local file count overflow"))?,
            providing_count: u32::try_from(snapshot.providing_count)
                .map_err(|_| Status::internal("providing count overflow"))?,
            active_downloads: u32::try_from(snapshot.active_downloads)
                .map_err(|_| Status::internal("active downloads count overflow"))?,
        }))
    }
}

fn peer_entry(snapshot: PeerSnapshot) -> PeerEntry {
    PeerEntry {
        peer_id: snapshot.peer_id.to_string(),
        addresses: snapshot.addresses.iter().map(ToString::to_string).collect(),
        connected: snapshot.connected,
        dialing: snapshot.dialing,
    }
}

fn to_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

#[allow(clippy::result_large_err)]
fn validate_cid(cid: String) -> Result<String, Status> {
    validate_cid_hex(&cid).map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(cid)
}

#[allow(clippy::result_large_err)]
fn parse_path(path: &str, field: &str) -> Result<PathBuf, Status> {
    if path.trim().is_empty() {
        return Err(Status::invalid_argument(format!("{field} cannot be empty")));
    }

    Ok(PathBuf::from(path))
}

#[cfg(test)]
mod tests {
    use super::{parse_path, validate_cid};

    #[test]
    fn reject_empty_path_arguments() {
        let result = parse_path("   ", "output_path");
        assert!(result.is_err());
    }

    #[test]
    fn reject_invalid_cid_arguments() {
        let result = validate_cid("abc".to_string());
        assert!(result.is_err());
    }
}
