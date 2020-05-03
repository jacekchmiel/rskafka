mod array;
mod bytes;
mod int;
mod string;

use crate::{wire_format::*, ParseError};
use nom::{
    bytes::complete::take,
    combinator::{map, map_res},
    number::complete::be_u8,
};
use std::convert::TryInto;
pub use string::{CompactNullableString, NullableString};
use uuid::Uuid;

impl KafkaWireFormatParse for bool {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        map_res(be_u8, |byte| match byte {
            0u8 => Ok(false),
            1u8 => Ok(true),
            _ => Err(ParseError::Custom("invalid bool value".into())),
        })(input)
    }
}

impl KafkaWireFormatWrite for bool {
    fn serialized_size(&self) -> usize {
        Self::serialized_size_static()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let byte = if *self { 0x01 } else { 0x00 };
        writer.write_all(&[byte]).map(|_| ())
    }
}

impl KafkaWireFormatStaticSize for bool {
    fn serialized_size_static() -> usize {
        std::mem::size_of::<u8>()
    }
}

impl KafkaWireFormatParse for Uuid {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        map(take(16usize), |bytes: &[u8]| {
            Uuid::from_bytes(bytes.try_into().unwrap())
        })(input)
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
