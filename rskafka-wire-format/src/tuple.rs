use crate::error::ParseError;
use crate::prelude::*;
use nom::sequence::tuple;

impl<T1, T2> WireFormatParse for (T1, T2)
where
    T1: WireFormatParse,
    T2: WireFormatParse,
{
    fn parse(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        tuple((T1::parse, T2::parse))(input)
    }
}
impl<T1, T2> WireFormatWrite for (T1, T2)
where
    T1: WireFormatWrite,
    T2: WireFormatWrite,
{
    fn wire_size(&self) -> usize {
        self.0.wire_size() + self.1.wire_size()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.0.write_into(writer)?;
        self.1.write_into(writer)?;

        Ok(())
    }
}
