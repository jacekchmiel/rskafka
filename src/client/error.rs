use crate::proto::{data::error::ErrorCode, ParseError};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("kafka client error")]
pub enum Error {
    #[error("missing {0} in config")]
    IncompleteConfig(&'static str),

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("response parse error")]
    ParseError(#[from] ParseError),

    #[error("protocol error: {0}")]
    ProtocolError(String),

    #[error("received error response")]
    ErrorResponse(ErrorCode),
}
