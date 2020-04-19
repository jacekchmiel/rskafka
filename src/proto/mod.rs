pub(crate) mod apis;
pub(crate) mod data;
pub(crate) mod decode;

use data::{api_key::ApiKey, header::ResponseHeader};
use nom::IResult;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("parse error")]
pub enum ParseError {
    #[error("too many bytes")]
    TooMuchData(usize),
    #[error("not enough bytes")]
    Incomplete(nom::Needed),
    #[error("{0:?}")]
    Parse(nom::error::ErrorKind),
}

impl From<nom::Err<(&[u8], nom::error::ErrorKind)>> for ParseError {
    fn from(v: nom::Err<(&[u8], nom::error::ErrorKind)>) -> Self {
        match v {
            nom::Err::Incomplete(needed) => ParseError::Incomplete(needed),
            nom::Err::Error((_, e)) | nom::Err::Failure((_, e)) => ParseError::Parse(e),
        }
    }
}

// pub(crate) fn parse_response(input: &[u8]) -> Result<Response, ParseError> {
//     match decode::response(input) {
//         Ok((&[], rsp)) => Ok(rsp),
//         Ok((rem, _)) => Err(ParseError::TooMuchData(rem.len())),
//         Err(nom::Err::Incomplete(needed)) => Err(ParseError::Incomplete(needed)),
//         Err(nom::Err::Error((_, e))) | Err(nom::Err::Failure((_, e))) => Err(ParseError::Parse(e)),
//     }
// }

pub(crate) trait KafkaWireFormatWrite {
    fn serialized_size(&self) -> usize;
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<usize>;

    #[cfg(test)]
    fn to_wire_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.serialized_size());
        self.write_into(&mut buffer)
            .expect("write into buffer failed");

        buffer
    }
}

pub(crate) trait KafkaRequest: KafkaWireFormatWrite {
    const API_KEY: ApiKey;
    const API_VERSION: i16;
}

pub(crate) trait KafkaWireFormatParse: Sized {
    fn parse_bytes(input: &[u8]) -> IResult<&[u8], Self>;

    fn from_wire_bytes(input: &[u8]) -> Result<Self, ParseError> {
        match Self::parse_bytes(input) {
            Ok((&[], parsed)) => Ok(parsed),
            Ok((rem, _)) => Err(ParseError::TooMuchData(rem.len())),
            Err(nom::Err::Incomplete(needed)) => Err(ParseError::Incomplete(needed)),
            Err(nom::Err::Error((_, e))) | Err(nom::Err::Failure((_, e))) => {
                Err(ParseError::Parse(e))
            }
        }
    }
}

pub(crate) fn parse_header(input: &[u8]) -> Result<(&[u8], ResponseHeader), ParseError> {
    match ResponseHeader::parse_bytes(input) {
        Ok((response_bytes, header)) => Ok((response_bytes, header)),
        Err(nom::Err::Incomplete(needed)) => Err(ParseError::Incomplete(needed)),
        Err(nom::Err::Error((_, e))) | Err(nom::Err::Failure((_, e))) => Err(ParseError::Parse(e)),
    }
}
