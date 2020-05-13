use rskafka_proto::{ApiKey, ErrorCode};
use rskafka_wire_format::error::ParseError;
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
    ProtocolError(Cow<'static, str>),

    #[error("received error response: {0} {1}")]
    ErrorResponse(ErrorCode, Cow<'static, str>),

    #[error("api not supported {:0?}, version {1}")]
    ApiNotSupported(ApiKey, i16),

    #[error("value error: {0}")]
    ValueError(Cow<'static, str>),

    #[error("cluster error: {0}")]
    ClusterError(String),
}

impl From<(ErrorCode, Option<String>)> for Error {
    fn from(v: (ErrorCode, Option<String>)) -> Self {
        Error::ErrorResponse(v.0, v.1.map(Into::into).unwrap_or("".into()))
    }
}

impl From<ErrorCode> for Error {
    fn from(v: ErrorCode) -> Self {
        (v, None).into()
    }
}
