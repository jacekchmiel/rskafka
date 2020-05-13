use std::borrow::Cow;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("parse error")]
pub enum ParseError {
    #[error("too many bytes")]
    TooMuchData(usize),
    #[error("not enough bytes")]
    Incomplete(nom::Needed),
    #[error("cannot interpret protocol bytes in: {0:?}")]
    Parse(nom::error::ErrorKind),
    #[error("{0}")]
    Custom(Cow<'static, str>),
    #[error("while parsing {0}: {1}")]
    Context(Cow<'static, str>, Box<ParseError>),
}

impl ParseError {
    pub fn context(self, ctx: Cow<'static, str>) -> Self {
        ParseError::Context(ctx, Box::new(self))
    }
}

impl<I> nom::error::ParseError<I> for ParseError {
    fn from_error_kind(_input: I, kind: nom::error::ErrorKind) -> Self {
        ParseError::Parse(kind)
    }

    fn append(_input: I, kind: nom::error::ErrorKind, other: Self) -> Self {
        // other
        ParseError::Context(kind.description().to_string().into(), Box::new(other))
    }
}

// impl From<nom::Err<(&[u8], nom::error::ErrorKind)>> for ParseError {
//     fn from(v: nom::Err<(&[u8], nom::error::ErrorKind)>) -> Self {
//         match v {
//             nom::Err::Incomplete(needed) => ParseError::Incomplete(needed),
//             nom::Err::Error((i, e)) | nom::Err::Failure((i, e)) => {
//                 ParseError::from_error_kind(i, e)
//             }
//         }
//     }
// }

impl From<nom::Err<ParseError>> for ParseError {
    fn from(v: nom::Err<ParseError>) -> Self {
        match v {
            nom::Err::Incomplete(needed) => ParseError::Incomplete(needed),
            nom::Err::Error(e) | nom::Err::Failure(e) => e,
        }
    }
}

pub fn custom_error(s: &'static str) -> nom::Err<ParseError> {
    nom::Err::Error(ParseError::Custom(s.into()))
}

pub fn custom_io_error<E: std::error::Error + Send + Sync + 'static>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}
