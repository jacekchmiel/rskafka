use crate::error::ParseError;
use crate::prelude::*;
use nom::combinator::map_res;
use nom::number::complete::be_u8;

impl WireFormatParse for bool {
    fn parse(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        map_res(be_u8, |byte| match byte {
            0u8 => Ok(false),
            1u8 => Ok(true),
            _ => Err(ParseError::Custom("invalid bool value".into())),
        })(input)
    }
}

impl WireFormatWrite for bool {
    fn wire_size(&self) -> usize {
        Self::wire_size_static()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let byte = if *self { 0x01 } else { 0x00 };
        writer.write_all(&[byte]).map(|_| ())
    }
}

impl WireFormatSizeStatic for bool {
    fn wire_size_static() -> usize {
        std::mem::size_of::<u8>()
    }
}
