use crate::config::PROTOCOL_NAME;
use libp2p::ping;
use libp2p::request_response::{self, json, ProtocolSupport};
use libp2p::swarm::NetworkBehaviour;
use libp2p::StreamProtocol;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageRequest {
    pub message: String,
    #[serde(default = "default_timestamp")]
    pub timestamp: u64,
}

fn default_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl MessageRequest {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            timestamp: default_timestamp(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageResponse {
    pub ack: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl MessageResponse {
    pub fn ok() -> Self {
        Self {
            ack: true,
            error: None,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            ack: false,
            error: Some(message.into()),
        }
    }
}

impl Default for MessageResponse {
    fn default() -> Self {
        Self::ok()
    }
}

#[derive(NetworkBehaviour)]
pub struct ChatBehaviour {
    pub ping: ping::Behaviour,
    pub messaging: json::Behaviour<MessageRequest, MessageResponse>,
}

impl ChatBehaviour {
    pub fn new(ping_interval: Duration) -> Self {
        let ping_config = ping::Config::new().with_interval(ping_interval);

        let messaging_config =
            request_response::Config::default().with_request_timeout(Duration::from_secs(30));

        Self {
            ping: ping::Behaviour::new(ping_config),
            messaging: json::Behaviour::new(
                [(StreamProtocol::new(PROTOCOL_NAME), ProtocolSupport::Full)],
                messaging_config,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_request_new() {
        let msg = MessageRequest::new("Hello, World!");
        assert_eq!(msg.message, "Hello, World!");
        assert!(msg.timestamp > 0);
    }

    #[test]
    fn test_message_response_ok() {
        let response = MessageResponse::ok();
        assert!(response.ack);
        assert!(response.error.is_none());
    }

    #[test]
    fn test_message_response_error() {
        let response = MessageResponse::error("Something went wrong");
        assert!(!response.ack);
        assert_eq!(response.error, Some("Something went wrong".to_string()));
    }
}
