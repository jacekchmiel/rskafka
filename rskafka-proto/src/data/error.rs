use crate::{wire_format::*, ParseError};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ErrorCode(pub i16);

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "error_code={}", self.0)
    }
}

impl<'a> KafkaWireFormatParse for ErrorCode {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        use nom::combinator::map;

        map(i16::parse_bytes, ErrorCode)(input)
    }
}
