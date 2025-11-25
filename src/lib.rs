pub mod behaviour;
pub mod config;
pub mod error;

pub use behaviour::{ChatBehaviour, ChatBehaviourEvent, MessageRequest, MessageResponse};
pub use config::Config;
pub use error::{ChatError, Result};
