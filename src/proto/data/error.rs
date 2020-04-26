use crate::{
    proto::{KafkaWireFormatParse, ParseError},
    KafkaError,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ErrorCode(pub(crate) i16);

impl ErrorCode {
    pub fn get_err(&self) -> Option<KafkaError> {
        match self.0 {
            0 => None,
            code => Some(KafkaError(code)),
        }
    }
}

impl<'a> KafkaWireFormatParse for ErrorCode {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        use nom::combinator::map;

        map(i16::parse_bytes, ErrorCode)(input)
    }
}
