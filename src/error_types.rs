use thiserror::Error;

#[derive(Debug, Error)]
pub enum BotError {
    #[error("Gmo Client message found: {}, {}", .code, .message)]
    GmoClientMessage {code: String, message: String},
    #[error("Maintenance")]
    Maintenance,
}
