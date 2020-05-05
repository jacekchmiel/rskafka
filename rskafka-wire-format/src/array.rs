use crate::error::ParseError;
use crate::prelude::*;
use nom::{
    combinator::{map, map_res},
    multi::many_m_n,
};
use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
};

impl<T> WireFormatParse for Vec<T>
where
    T: WireFormatParse,
{
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        let (input, size) = map_res(i32::parse_bytes, try_into_usize)(input)?;
        many_m_n(size, size, T::parse_bytes)(input)
    }
}

fn try_into_usize(v: i32) -> Result<usize, ParseError> {
    v.try_into()
        .map_err(|_| ParseError::Custom("negative array size".into()))
}

impl<T> WireFormatParse for Cow<'static, [T]>
where
    T: WireFormatParse + Clone,
{
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        map(Vec::<T>::parse_bytes, Cow::Owned)(input)
    }
}

impl<T> WireFormatWrite for [T]
where
    T: WireFormatWrite,
{
    fn wire_size(&self) -> usize {
        let len_size = i32::wire_size_static();
        let fields_size: usize = self.iter().map(|v| v.wire_size()).sum::<usize>();

        len_size + fields_size
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        i32::try_from(self.len())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
            .write_into(writer)?;

        for item in self.iter() {
            item.write_into(writer)?;
        }

        Ok(())
    }
}

impl<'a, T> WireFormatWrite for Cow<'a, [T]>
where
    T: WireFormatWrite + Clone,
{
    fn wire_size(&self) -> usize {
        self.as_ref().wire_size()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.as_ref().write_into(writer)
    }
}

impl<T> WireFormatWrite for Vec<T>
where
    T: WireFormatWrite,
{
    fn wire_size(&self) -> usize {
        self.as_slice().wire_size()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.as_slice().write_into(writer)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn array_write() {
        let array = vec![1i32, 2i32];
        let expected = vec![0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2];

        assert_eq!(array.wire_size(), expected.len());
        assert_eq!(array.to_wire_bytes(), expected);
    }
}
