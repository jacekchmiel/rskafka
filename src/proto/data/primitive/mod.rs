mod bytes;
mod int;
mod string;

use crate::proto::{KafkaWireFormatParse, ParseError};
use nom::{
    bytes::complete::take,
    combinator::{flat_map, map, map_res},
    multi::many_m_n,
    number::complete::be_u8,
};
use std::convert::TryInto;
pub use string::{CompactNullableString, NullableString};
use uuid::Uuid;

impl<'a> KafkaWireFormatParse<'a> for bool {
    fn parse_bytes(input: &'a [u8]) -> nom::IResult<&'a [u8], Self, crate::proto::ParseError> {
        map_res(be_u8, |byte| match byte {
            0u8 => Ok(false),
            1u8 => Ok(true),
            _ => Err(ParseError::Custom("invalid bool value")),
        })(input)
    }
}

impl<'a> KafkaWireFormatParse<'a> for Uuid {
    fn parse_bytes(input: &'a [u8]) -> nom::IResult<&'a [u8], Self, ParseError> {
        map(take(16usize), |bytes: &[u8]| {
            Uuid::from_bytes(bytes.try_into().unwrap())
        })(input)
    }
}

impl<'a, T> KafkaWireFormatParse<'a> for Vec<T>
where
    T: KafkaWireFormatParse<'a>,
{
    fn parse_bytes(input: &'a [u8]) -> nom::IResult<&'a [u8], Self, ParseError> {
        flat_map(map_res(i32::parse_bytes, try_into_usize), |cnt| {
            many_m_n(cnt, cnt, T::parse_bytes)
        })(input)
    }
}

fn try_into_usize<T: TryInto<usize>>(v: T) -> Result<usize, ParseError> {
    v.try_into()
        .map_err(|_| ParseError::Custom("negative size"))
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
