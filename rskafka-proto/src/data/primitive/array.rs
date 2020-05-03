use crate::{
    wire_format::{KafkaWireFormatParse, KafkaWireFormatWrite},
    ParseError,
};
use nom::{
    combinator::{flat_map, map_res},
    multi::many_m_n,
};
use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
};

impl<T> KafkaWireFormatParse for Vec<T>
where
    T: KafkaWireFormatParse,
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

impl<T> KafkaWireFormatWrite for [T]
where
    T: KafkaWireFormatWrite,
{
    fn serialized_size(&self) -> usize {
        let len = self.as_ref().len();
        let len_size = (len as i32).serialized_size();
        let fields_size: usize = len * self.iter().map(|v| v.serialized_size()).sum::<usize>();
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

impl<'a, T> KafkaWireFormatWrite for Cow<'a, [T]>
where
    T: KafkaWireFormatWrite + Clone,
{
    fn serialized_size(&self) -> usize {
        self.as_ref().serialized_size()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.as_ref().write_into(writer)
    }
}

impl<T> KafkaWireFormatWrite for Vec<T>
where
    T: KafkaWireFormatWrite,
{
    fn serialized_size(&self) -> usize {
        self.as_slice().serialized_size()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.as_slice().write_into(writer)
    }
}
