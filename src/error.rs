use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChatError {
    #[error("Network error: {0}")]
    Network(#[from] libp2p::TransportError<std::io::Error>),

    #[error("Invalid multiaddr: {0}")]
    InvalidMultiaddr(#[from] libp2p::multiaddr::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to dial peer: {0}")]
    Dial(#[from] libp2p::swarm::DialError),

    #[error("Failed to listen: {0}")]
    Listen(#[from] libp2p::swarm::ListenError),

    #[error("Noise protocol error: {0}")]
    Noise(#[from] libp2p::noise::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, ChatError>;

impl From<String> for ChatError {
    fn from(s: String) -> Self {
        ChatError::Other(s)
    }
}

impl From<&str> for ChatError {
    fn from(s: &str) -> Self {
        ChatError::Other(s.to_string())
    }
}
