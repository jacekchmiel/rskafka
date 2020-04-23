use crate::proto::{KafkaWireFormatParse, ParseError};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ErrorCode(i16);

impl ErrorCode {
    pub fn from_i16(v: i16) -> Option<Self> {
        if v == 0 {
            None
        } else {
            Some(ErrorCode(v))
        }
    }

    pub fn to_i16(&self) -> i16 {
        self.0
    }
}

impl<'a> KafkaWireFormatParse<'a> for Option<ErrorCode> {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        use nom::combinator::map;
        use nom::number::complete::le_i16;

        map(le_i16, ErrorCode::from_i16)(input)
    }
}
