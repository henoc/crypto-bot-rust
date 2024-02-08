use hyper::StatusCode;
use labo::export::thiserror::Error;

#[derive(Debug, Error)]
pub enum BotError {
    #[error("Gmo Client message found: {}, {}", .code, .message)]
    GmoClientMessage {code: String, message: String},
    #[error("Bitflyer Client message found: {}, {}, {}", .status, .message, .reqest)]
    BitflyerClientMessage {status: StatusCode, message: String, reqest: String},
    #[error("Maintenance")]
    Maintenance,
    #[error("Margin insufficient")]
    MarginInsufficiency,
    #[error("Too many request in websocket")]
    WsTooManyRequest,
}
