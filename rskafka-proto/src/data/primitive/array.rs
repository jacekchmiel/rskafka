use crate::{wire_format::*, ParseError};
use nom::{
    combinator::{flat_map, map_res},
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
        flat_map(map_res(i32::parse_bytes, try_into_usize), |cnt| {
            many_m_n(cnt, cnt, T::parse_bytes)
        })(input)
    }
}

fn try_into_usize<T: TryInto<usize>>(v: T) -> Result<usize, ParseError> {
    v.try_into()
        .map_err(|_| ParseError::Custom("negative array size".into()))
}

impl<T> WireFormatWrite for [T]
where
    T: WireFormatWrite,
{
    fn wire_size(&self) -> usize {
        let len = self.as_ref().len();
        let len_size = (len as i32).wire_size();
        let fields_size: usize = len * self.iter().map(|v| v.wire_size()).sum::<usize>();
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
