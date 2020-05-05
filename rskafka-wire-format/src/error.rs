use nom::error::ParseError as ParseErrorTrait;
use std::borrow::Cow;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("parse error")]
pub enum ParseError {
    #[error("too many bytes")]
    TooMuchData(usize),
    #[error("not enough bytes")]
    Incomplete(nom::Needed),
    #[error("{0:?}")]
    Parse(Vec<nom::error::ErrorKind>),
    #[error("{0:?}")]
    Custom(Cow<'static, str>),
}

impl<I> nom::error::ParseError<I> for ParseError {
    fn from_error_kind(_input: I, kind: nom::error::ErrorKind) -> Self {
        ParseError::Parse(vec![kind])
    }

    fn append(_input: I, kind: nom::error::ErrorKind, other: Self) -> Self {
        match other {
            ParseError::Parse(trace) => {
                ParseError::Parse(trace.into_iter().chain(std::iter::once(kind)).collect())
            }
            other => other,
        }
    }
}

impl From<nom::Err<(&[u8], nom::error::ErrorKind)>> for ParseError {
    fn from(v: nom::Err<(&[u8], nom::error::ErrorKind)>) -> Self {
        match v {
            nom::Err::Incomplete(needed) => ParseError::Incomplete(needed),
            nom::Err::Error((i, e)) | nom::Err::Failure((i, e)) => {
                ParseError::from_error_kind(i, e)
            }
        }
    }
}

pub fn custom_error(s: &'static str) -> nom::Err<ParseError> {
    nom::Err::Error(ParseError::Custom(s.into()))
}

pub fn custom_io_error<E: std::error::Error + Send + Sync + 'static>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}
