use crate::manifest::Manifest;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use libp2p::futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response::Codec;
use libp2p::StreamProtocol;
use serde::{Deserialize, Serialize};
use std::io;

pub const METADATA_PROTOCOL: &str = "/dfs/metadata/1.0.0";
pub const CHUNK_PROTOCOL: &str = "/dfs/chunk/1.0.0";

#[derive(Debug, Clone, Copy)]
pub struct ProtocolLimits {
    pub max_request_bytes: usize,
    pub max_metadata_response_bytes: usize,
    pub max_chunk_response_bytes: usize,
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_request_bytes: 8 * 1024,
            max_metadata_response_bytes: 1024 * 1024,
            max_chunk_response_bytes: 256 * 1024 + 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataRequest {
    pub cid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MetadataResponse {
    Found { manifest: Manifest },
    NotFound { error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkRequest {
    pub chunk_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChunkResponse {
    Found {
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
    },
    NotFound {
        error: String,
    },
}

#[derive(Debug, Clone)]
pub struct MetadataCodec {
    limits: ProtocolLimits,
}

impl MetadataCodec {
    pub fn new(limits: ProtocolLimits) -> Self {
        Self { limits }
    }

    fn validate_request(request: &MetadataRequest) -> io::Result<()> {
        validate_hex_id(&request.cid, "cid")
    }

    fn validate_response(&self, response: &MetadataResponse) -> io::Result<()> {
        match response {
            MetadataResponse::Found { manifest } => {
                if manifest.merkle_root.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "manifest has empty merkle root",
                    ));
                }
                Ok(())
            }
            MetadataResponse::NotFound { error } => {
                if error.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "metadata error cannot be empty",
                    ));
                }
                Ok(())
            }
        }
    }
}

#[async_trait]
impl Codec for MetadataCodec {
    type Protocol = StreamProtocol;
    type Request = MetadataRequest;
    type Response = MetadataResponse;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let payload = read_framed(io, self.limits.max_request_bytes).await?;
        let request: MetadataRequest = deserialize(&payload)?;
        Self::validate_request(&request)?;
        Ok(request)
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let payload = read_framed(io, self.limits.max_metadata_response_bytes).await?;
        let response: MetadataResponse = deserialize(&payload)?;
        self.validate_response(&response)?;
        Ok(response)
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        Self::validate_request(&req)?;
        let payload = serialize(&req)?;
        write_framed(io, &payload, self.limits.max_request_bytes).await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        self.validate_response(&res)?;
        let payload = serialize(&res)?;
        write_framed(io, &payload, self.limits.max_metadata_response_bytes).await
    }
}

#[derive(Debug, Clone)]
pub struct ChunkCodec {
    limits: ProtocolLimits,
}

impl ChunkCodec {
    pub fn new(limits: ProtocolLimits) -> Self {
        Self { limits }
    }

    fn validate_request(request: &ChunkRequest) -> io::Result<()> {
        validate_hex_id(&request.chunk_hash, "chunk_hash")
    }

    fn validate_response(&self, response: &ChunkResponse) -> io::Result<()> {
        match response {
            ChunkResponse::Found { bytes } => {
                if bytes.len() > self.limits.max_chunk_response_bytes {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "chunk response exceeds max size",
                    ));
                }
                Ok(())
            }
            ChunkResponse::NotFound { error } => {
                if error.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "chunk error cannot be empty",
                    ));
                }
                Ok(())
            }
        }
    }
}

#[async_trait]
impl Codec for ChunkCodec {
    type Protocol = StreamProtocol;
    type Request = ChunkRequest;
    type Response = ChunkResponse;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let payload = read_framed(io, self.limits.max_request_bytes).await?;
        let request: ChunkRequest = deserialize(&payload)?;
        Self::validate_request(&request)?;
        Ok(request)
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let payload = read_framed(io, self.limits.max_chunk_response_bytes).await?;
        let response: ChunkResponse = deserialize(&payload)?;
        self.validate_response(&response)?;
        Ok(response)
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        Self::validate_request(&req)?;
        let payload = serialize(&req)?;
        write_framed(io, &payload, self.limits.max_request_bytes).await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        self.validate_response(&res)?;
        let payload = serialize(&res)?;
        write_framed(io, &payload, self.limits.max_chunk_response_bytes).await
    }
}

pub fn chunk_response_from_bytes(bytes: Bytes) -> ChunkResponse {
    ChunkResponse::Found {
        bytes: bytes.to_vec(),
    }
}

pub fn chunk_bytes(response: &ChunkResponse) -> Option<Bytes> {
    match response {
        ChunkResponse::Found { bytes } => Some(Bytes::copy_from_slice(bytes)),
        ChunkResponse::NotFound { .. } => None,
    }
}

fn serialize<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    bincode::serialize(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

fn deserialize<T: for<'a> Deserialize<'a>>(payload: &[u8]) -> io::Result<T> {
    bincode::deserialize(payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

async fn read_framed<T>(io: &mut T, max_bytes: usize) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin + Send,
{
    let mut len_bytes = [0_u8; 4];
    io.read_exact(&mut len_bytes).await?;
    let frame_len = u32::from_be_bytes(len_bytes) as usize;

    if frame_len > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {} exceeds max {}", frame_len, max_bytes),
        ));
    }

    let mut payload = vec![0_u8; frame_len];
    io.read_exact(&mut payload).await?;
    Ok(payload)
}

async fn write_framed<T>(io: &mut T, payload: &[u8], max_bytes: usize) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    if payload.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("payload length {} exceeds max {}", payload.len(), max_bytes),
        ));
    }

    let frame_len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "payload too large"))?;
    io.write_all(&frame_len.to_be_bytes()).await?;
    io.write_all(payload).await?;
    io.flush().await?;
    Ok(())
}

fn validate_hex_id(value: &str, field: &str) -> io::Result<()> {
    if value.is_empty() || value.len() > 128 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} has invalid length"),
        ));
    }

    if value.chars().any(|c| !c.is_ascii_hexdigit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} must be hex encoded"),
        ));
    }

    Ok(())
}

pub fn validate_manifest_cid(manifest: &Manifest, expected_cid: &str) -> Result<()> {
    if manifest.merkle_root != expected_cid {
        anyhow::bail!(
            "manifest merkle root mismatch: expected {}, got {}",
            expected_cid,
            manifest.merkle_root
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ChunkCodec, ChunkRequest, MetadataCodec, MetadataRequest, ProtocolLimits, CHUNK_PROTOCOL,
        METADATA_PROTOCOL,
    };
    use libp2p::futures::io::Cursor;
    use libp2p::request_response::Codec;
    use libp2p::StreamProtocol;

    #[tokio::test]
    async fn metadata_codec_rejects_oversized_request() {
        let mut codec = MetadataCodec::new(ProtocolLimits {
            max_request_bytes: 16,
            max_metadata_response_bytes: 1024,
            max_chunk_response_bytes: 1024,
        });

        let protocol = StreamProtocol::new(METADATA_PROTOCOL);
        let mut frame = Vec::new();
        frame.extend_from_slice(&(20_u32.to_be_bytes()));
        frame.extend_from_slice(&vec![1_u8; 20]);
        let mut reader = Cursor::new(frame);

        let request = codec.read_request(&protocol, &mut reader).await;
        assert!(request.is_err());
    }

    #[tokio::test]
    async fn chunk_codec_rejects_oversized_response() {
        let mut codec = ChunkCodec::new(ProtocolLimits {
            max_request_bytes: 1024,
            max_metadata_response_bytes: 1024,
            max_chunk_response_bytes: 64,
        });

        let protocol = StreamProtocol::new(CHUNK_PROTOCOL);
        let mut frame = Vec::new();
        frame.extend_from_slice(&(72_u32.to_be_bytes()));
        frame.extend_from_slice(&vec![2_u8; 72]);
        let mut reader = Cursor::new(frame);

        let response = codec.read_response(&protocol, &mut reader).await;
        assert!(response.is_err());
    }

    #[tokio::test]
    async fn metadata_roundtrip() {
        let limits = ProtocolLimits::default();
        let mut writer_codec = MetadataCodec::new(limits);
        let mut reader_codec = MetadataCodec::new(limits);
        let protocol = StreamProtocol::new(METADATA_PROTOCOL);
        let mut io = Cursor::new(Vec::<u8>::new());

        let request = MetadataRequest {
            cid: "0".repeat(64),
        };

        let wrote = writer_codec
            .write_request(&protocol, &mut io, request.clone())
            .await;
        assert!(wrote.is_ok());

        io.set_position(0);
        let read = reader_codec.read_request(&protocol, &mut io).await;
        assert!(read.is_ok());

        if let Ok(read) = read {
            assert_eq!(read, request);
        }
    }

    #[tokio::test]
    async fn chunk_request_roundtrip() {
        let limits = ProtocolLimits::default();
        let mut writer_codec = ChunkCodec::new(limits);
        let mut reader_codec = ChunkCodec::new(limits);
        let protocol = StreamProtocol::new(CHUNK_PROTOCOL);
        let mut io = Cursor::new(Vec::<u8>::new());

        let request = ChunkRequest {
            chunk_hash: "a".repeat(64),
        };

        let wrote = writer_codec
            .write_request(&protocol, &mut io, request.clone())
            .await;
        assert!(wrote.is_ok());

        io.set_position(0);
        let read = reader_codec.read_request(&protocol, &mut io).await;
        assert!(read.is_ok());

        if let Ok(read) = read {
            assert_eq!(read, request);
        }
    }
}
