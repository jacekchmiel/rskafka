use super::int::VarInt;
use crate::proto::{custom_error, KafkaWireFormatParse, ParseError};
use nom::bytes::complete::take;
use nom::combinator::map;
use std::borrow::Cow;
use std::convert::TryFrom;

impl KafkaWireFormatParse for Vec<u8> {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        let (input, ssize) = i32::parse_bytes(input)?;
        let size = usize::try_from(ssize).map_err(|_| custom_error("negative size"))?;
        map(take(size), |bytes: &[u8]| Vec::from(bytes))(input)
    }
}

pub struct CompactBytes(pub Vec<u8>);

impl<'a> KafkaWireFormatParse for CompactBytes {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        let (input, VarInt(size_encoded)) = VarInt::parse_bytes(input)?;
        let size = usize::try_from(size_encoded - 1).map_err(|_| custom_error("negative size"))?;
        let (input, bytes) = take(size)(input)?;
        Ok((input, CompactBytes(Vec::from(bytes))))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct NullableBytes(Option<Vec<u8>>);

impl NullableBytes {
    pub fn with_null() -> Self {
        NullableBytes(None)
    }

    pub fn with_data(b: &[u8]) -> Self {
        NullableBytes(Some(Vec::from(b)))
    }
}

impl KafkaWireFormatParse for NullableBytes {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        let (input, ssize) = i32::parse_bytes(input)?;
        match ssize {
            -1 => Ok((input, NullableBytes::with_null())),
            other if other < 0 => panic!(), //FIXME: error
            other => {
                let count = other as usize;
                // let (_, bytes) = take(count)(input);
                map(take(count), NullableBytes::with_data)(input)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_bytes() {
        let expected: Vec<u8> = vec![1, 2, 3];
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            Vec::<u8>::parse_bytes(&[0, 0, 0, 3, 1, 2, 3, 4, 5, 6]),
            Ok((remaining, (expected)))
        );
    }

    #[test]
    fn parse_nullable_bytes_null() {
        let remaining: &[u8] = &[1, 2, 3, 4, 5, 6];
        assert_eq!(
            NullableBytes::parse_bytes(&[255, 255, 255, 255, 1, 2, 3, 4, 5, 6]),
            Ok((remaining, NullableBytes::with_null()))
        );
    }

    #[test]
    fn parse_nullable_bytes_empty() {
        let remaining: &[u8] = &[1, 2, 3, 4, 5, 6];
        assert_eq!(
            NullableBytes::parse_bytes(&[0, 0, 0, 0, 1, 2, 3, 4, 5, 6]),
            Ok((remaining, NullableBytes::with_data(&[])))
        );
    }

    #[test]
    fn parse_nullable_bytes() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            NullableBytes::parse_bytes(&[0, 0, 0, 3, 1, 2, 3, 4, 5, 6]),
            Ok((remaining, NullableBytes::with_data(&[1, 2, 3])))
        );
    }
}
