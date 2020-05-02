pub mod api_key;
pub mod error;
pub mod header;
pub mod primitive;

use crate::wire_format::*;
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct BrokerId(pub(crate) i32);

impl KafkaWireFormatParse for BrokerId {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, super::ParseError> {
        nom::combinator::map(i32::parse_bytes, BrokerId)(input)
    }
}

impl std::fmt::Display for BrokerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
