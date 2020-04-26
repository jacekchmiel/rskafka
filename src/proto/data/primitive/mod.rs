mod bytes;
mod int;
mod string;

use crate::proto::{KafkaWireFormatParse, KafkaWireFormatWrite, ParseError};
use nom::{
    bytes::complete::take,
    combinator::{flat_map, map, map_res},
    multi::many_m_n,
    number::complete::be_u8,
};
use std::convert::{TryFrom, TryInto};
pub use string::{CompactNullableString, NullableString};
use uuid::Uuid;

impl KafkaWireFormatParse for bool {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        map_res(be_u8, |byte| match byte {
            0u8 => Ok(false),
            1u8 => Ok(true),
            _ => Err(ParseError::Custom("invalid bool value")),
        })(input)
    }
}

impl KafkaWireFormatParse for Uuid {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        map(take(16usize), |bytes: &[u8]| {
            Uuid::from_bytes(bytes.try_into().unwrap())
        })(input)
    }
}

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
        .map_err(|_| ParseError::Custom("negative size"))
}

impl<T> KafkaWireFormatWrite for Vec<T>
where
    T: KafkaWireFormatWrite,
{
    fn serialized_size(&self) -> usize {
        let len = self.len();
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_uuid() {
        let empty: &[u8] = &[];
        assert_eq!(
            Uuid::parse_bytes(&[
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff
            ])
            .unwrap(),
            (
                empty,
                "00112233-4455-6677-8899-aabbccddeeff".parse().unwrap()
            )
        );
    }
}
