use clap::Parser;
use libp2p::Multiaddr;
use std::time::Duration;

pub const DEFAULT_PORT: u16 = 9999;

pub const DEFAULT_PING_INTERVAL_SECS: u64 = 10;

pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 30;

pub const PROTOCOL_NAME: &str = "/p2p-chat/1.0.0";

#[derive(Parser, Debug, Clone)]
#[command(
    name = "p2p-chat",
    author,
    version,
    about = "A peer-to-peer chat application built with libp2p",
    long_about = None
)]
pub struct Config {
    #[arg(short, long, env = "CHAT_P2P_PORT", default_value_t = DEFAULT_PORT)]
    pub port: u16,

    #[arg(short = 'c', long, env = "CHAT_PEER")]
    pub peer: Option<Multiaddr>,

    #[arg(long, default_value_t = DEFAULT_PING_INTERVAL_SECS)]
    pub ping_interval: u64,

    #[arg(long, default_value_t = DEFAULT_IDLE_TIMEOUT_SECS)]
    pub idle_timeout: u64,

    #[arg(short, long, default_value_t = false)]
    pub verbose: bool,

    #[arg(long, default_value = "info")]
    pub log_level: String,
}

impl Config {
    pub fn parse_args() -> Self {
        Self::parse()
    }

    pub fn listen_addr(&self) -> String {
        format!("/ip4/0.0.0.0/tcp/{}", self.port)
    }

    pub fn ping_interval_duration(&self) -> Duration {
        Duration::from_secs(self.ping_interval)
    }

    pub fn idle_timeout_duration(&self) -> Duration {
        Duration::from_secs(self.idle_timeout)
    }

    pub fn log_filter(&self) -> String {
        if self.verbose {
            "debug".to_string()
        } else {
            self.log_level.clone()
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            peer: None,
            ping_interval: DEFAULT_PING_INTERVAL_SECS,
            idle_timeout: DEFAULT_IDLE_TIMEOUT_SECS,
            verbose: false,
            log_level: "info".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.port, DEFAULT_PORT);
        assert!(config.peer.is_none());
        assert_eq!(config.ping_interval, DEFAULT_PING_INTERVAL_SECS);
    }

    #[test]
    fn test_listen_addr() {
        let config = Config::default();
        assert_eq!(config.listen_addr(), "/ip4/0.0.0.0/tcp/9999");
    }

    #[test]
    fn test_ping_interval_duration() {
        let config = Config::default();
        assert_eq!(
            config.ping_interval_duration(),
            Duration::from_secs(DEFAULT_PING_INTERVAL_SECS)
        );
    }
}
