use crate::{
    proto::{
        data::{api_key::ApiKey, error::ErrorCode},
        ParseError,
    },
    KafkaError,
};
use std::borrow::Cow;
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
    ErrorResponse(KafkaError),

    #[error("api not supported {:0?}, version {1}")]
    ApiNotSupported(ApiKey, i16),

    #[error("value error: {0}")]
    ValueError(Cow<'static, str>),
}
