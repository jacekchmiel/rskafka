pub(crate) mod apis;
pub(crate) mod data;

use data::api_key::ApiKey;
use nom::{error::ParseError as ParseErrorTrait, IResult};
use thiserror::Error;

/// Object that can be written in Kafka Protocol wire format
pub(crate) trait KafkaWireFormatWrite {
    fn serialized_size(&self) -> usize;
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()>;

    /// Writes data to new byte vector
    #[cfg(test)]
    fn to_wire_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.serialized_size());
        self.write_into(&mut buffer)
            .expect("write into buffer failed");

        buffer
    }
}

pub(crate) trait KafkaWireFormatStaticSize {
    fn serialized_size_static() -> usize;
}

/// Represents a concrete request in Kafka Protocol wire format.
/// Has specific Api Key, Api Version and can be written in wire format.
pub(crate) trait KafkaRequest: KafkaWireFormatWrite {
    const API_KEY: ApiKey;
    const API_VERSION: i16;

    fn api_key(&self) -> ApiKey {
        Self::API_KEY
    }

    fn api_version(&self) -> i16 {
        Self::API_VERSION
    }
}

/// Object that can be parsed from Kafka Protocol wire data
pub(crate) trait KafkaWireFormatParse: Sized {
    /// Parses bytes to create Self, borrowing data from buffer (lifetime of created object
    /// is bound to buffer lifetime). Follows nom protocol for easy parser combinations.
    fn parse_bytes(input: &[u8]) -> IResult<&[u8], Self, ParseError>;

    /// Creates object from exact number of bytes (can return ParseError::TooMuchData)
    fn from_wire_bytes(input: &[u8]) -> Result<Self, ParseError> {
        match Self::parse_bytes(input) {
            Ok((&[], parsed)) => Ok(parsed),
            Ok((rem, _)) => Err(ParseError::TooMuchData(rem.len())),
            Err(nom::Err::Incomplete(needed)) => Err(ParseError::Incomplete(needed)),
            Err(nom::Err::Error(error)) | Err(nom::Err::Failure(error)) => Err(error),
        }
    }

    /// Creates object from bytes. Does not return ParseError::TooMuchData if buffer contains more data.
    fn from_wire_bytes_buffer(input: &[u8]) -> Result<Self, ParseError> {
        match Self::parse_bytes(input) {
            Ok((_, parsed)) => Ok(parsed),
            Err(nom::Err::Incomplete(needed)) => Err(ParseError::Incomplete(needed)),
            Err(nom::Err::Error(error)) | Err(nom::Err::Failure(error)) => Err(error),
        }
    }
}

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
    Custom(&'static str),
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

fn custom_error(s: &'static str) -> nom::Err<ParseError> {
    nom::Err::Error(ParseError::Custom(s))
}

fn custom_io_error<E: std::error::Error + Send + Sync + 'static>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}
