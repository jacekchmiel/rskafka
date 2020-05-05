pub mod api_key;
pub mod error;
pub mod header;

use rskafka_wire_format::error::ParseError;
use rskafka_wire_format::prelude::*;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct BrokerId(pub(crate) i32);

impl WireFormatParse for BrokerId {
    fn parse_bytes(input: &[u8]) -> IResult<&[u8], Self, ParseError> {
        let (input, v) = i32::parse_bytes(input)?;
        Ok((input, BrokerId(v)))
    }
}

impl std::fmt::Display for BrokerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
