use crate::{data::header::RequestHeader, ApiKey, ParseError};
use log::{log_enabled, trace};
use nom::IResult;

/// Object that can be written in Kafka Protocol wire format
pub trait WireFormatWrite {
    fn wire_size(&self) -> usize;
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()>;

    /// Writes data to new byte vector
    #[cfg(test)]
    fn to_wire_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.wire_size());
        self.write_into(&mut buffer)
            .expect("write into buffer failed");
        buffer
    }
}

pub trait WireFormatSizeStatic {
    fn wire_size_static() -> usize;
}

/// Represents a concrete request in Kafka Protocol wire format.
/// Has specific Api Key, Api Version and can be written in wire format.
pub trait KafkaRequest: WireFormatWrite {
    const API_KEY: ApiKey;
    const API_VERSION: i16;
    type Response: KafkaResponse;

    fn write_bytes<W: std::io::Write>(
        &self,
        mut writer: W,
        correlation_id: i32,
        client_id: Option<&str>,
    ) -> std::io::Result<usize> {
        let header = RequestHeader {
            request_api_key: Self::API_KEY,
            request_api_version: Self::API_VERSION,
            correlation_id,
            client_id: client_id.into(),
        };
        let size = header.wire_size() + self.wire_size();

        trace!("write: size={:?}", size);
        trace!("write: header={:?}", header);

        if log_enabled!(log::Level::Trace) {
            let mut buffer = Vec::with_capacity(size as usize + std::mem::size_of_val(&size));
            (size as i32).write_into(&mut buffer)?; //TODO: conversion error
            header.write_into(&mut buffer)?;
            self.write_into(&mut buffer)?;
            trace!("write: {:?}", buffer);

            writer.write_all(&buffer)?;
        } else {
            (size as i32).write_into(&mut writer)?; //TODO: conversion error
            header.write_into(&mut writer)?;
            self.write_into(&mut writer)?;
        }

        writer.flush()?;

        Ok(0)
    }

    fn to_bytes(&self, correlation_id: i32, client_id: Option<&str>) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.write_bytes(&mut buffer, correlation_id, client_id)
            .expect("write to vec failed");

        buffer
    }
}

/// Object that can be parsed from Kafka Protocol wire data
pub trait WireFormatParse: Sized {
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

    // Hard-ish deprecation
    // /// Creates object from bytes. Does not return ParseError::TooMuchData if buffer contains more data.
    // fn from_wire_bytes_buffer(input: &[u8]) -> Result<Self, ParseError> {
    //     match Self::parse_bytes(input) {
    //         Ok((_, parsed)) => Ok(parsed),
    //         Err(nom::Err::Incomplete(needed)) => Err(ParseError::Incomplete(needed)),
    //         Err(nom::Err::Error(error)) | Err(nom::Err::Failure(error)) => Err(error),
    //     }
    // }
}

pub trait KafkaResponse: WireFormatParse {
    fn from_bytes(input: &[u8]) -> Result<Self, ParseError> {
        Self::from_wire_bytes(input)
    }
}
