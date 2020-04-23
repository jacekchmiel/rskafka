use crate::proto::{custom_error, KafkaWireFormatParse, KafkaWireFormatWrite};
use byteorder::{BigEndian, WriteBytesExt};
use nom::bytes::complete::take;
use std::borrow::Cow;
use std::convert::TryFrom;

impl<'a> KafkaWireFormatParse<'a> for Cow<'a, str> {
    fn parse_bytes(input: &'a [u8]) -> nom::IResult<&'a [u8], Self, crate::proto::ParseError> {
        let (input, size) = i16::parse_bytes(input)?;
        let size = usize::try_from(size).map_err(|_| custom_error("negative size"))?;

        let (input, bytes) = take(size)(input)?;
        let s = std::str::from_utf8(bytes).map_err(|_| custom_error("utf8 conversion failed"))?;

        Ok((input, Cow::Borrowed(s)))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct NullableString<'a>(pub Option<Cow<'a, str>>);

impl<'a> NullableString<'a> {
    pub fn owned(s: String) -> Self {
        NullableString(Some(Cow::Owned(s)))
    }

    pub fn borrowed(s: &'a str) -> Self {
        NullableString(Some(Cow::Borrowed(s)))
    }

    pub fn null() -> Self {
        NullableString(None)
    }
}

impl KafkaWireFormatWrite for NullableString<'_> {
    fn serialized_size(&self) -> usize {
        let size_size = std::mem::size_of::<i16>();
        let content_size = match self.0.as_ref() {
            None => 0,
            Some(string) => string.as_bytes().len(),
        };
        size_size + content_size
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<usize> {
        match self.0.as_ref() {
            None => writer
                .write_i16::<BigEndian>(-1)
                .map(|()| std::mem::size_of::<i16>()),
            Some(string) => {
                let bytes = string.as_bytes();
                writer.write_i16::<BigEndian>(bytes.len() as i16)?; //TODO: conversion error
                writer.write_all(bytes)?;
                Ok(bytes.len() + std::mem::size_of::<i16>())
            }
        }
    }
}

impl<'a> KafkaWireFormatParse<'a> for NullableString<'a> {
    fn parse_bytes(input: &'a [u8]) -> nom::IResult<&'a [u8], Self, crate::proto::ParseError> {
        let (input, ssize) = i16::parse_bytes(input)?;
        match ssize {
            -1 => Ok((input, NullableString::null())),
            other if other < 0 => panic!(), //FIXME: error
            other => {
                let count = other as usize;
                let (input, bytes) = take(count)(input)?;
                let string = std::str::from_utf8(bytes).unwrap(); // FIXME: error
                Ok((input, NullableString::borrowed(string)))
            }
        }
    }
}

pub struct CompactNullableString<'a>(pub Option<Cow<'a, str>>);

impl<'a> CompactNullableString<'a> {
    pub fn owned(s: String) -> Self {
        CompactNullableString(Some(Cow::Owned(s)))
    }

    pub fn borrowed(s: &'a str) -> Self {
        CompactNullableString(Some(Cow::Borrowed(s)))
    }

    pub fn null() -> Self {
        CompactNullableString(None)
    }
}

impl<'a> KafkaWireFormatParse<'a> for CompactNullableString<'a> {
    fn parse_bytes(_input: &'a [u8]) -> nom::IResult<&'a [u8], Self, crate::proto::ParseError> {
        // map(compact_nullable_bytes, |v| v.map(try_into_utf8))(input)
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_string() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            Cow::parse_bytes(&[0, 3, 0x61, 0x62, 0x63, 4, 5, 6]),
            Ok((remaining, Cow::Borrowed("abc")))
        );
    }

    #[test]
    fn parse_string_empty() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            Cow::parse_bytes(&[0, 0, 4, 5, 6]),
            Ok((remaining, Cow::Borrowed("")))
        );
    }

    #[test]
    fn nullable_string_to_wire_format() {
        let string = NullableString::borrowed("rskafka");
        assert_eq!(string.serialized_size(), 9);

        let bytes = string.to_wire_bytes();
        let expected = vec![0x00, 0x07, 0x72, 0x73, 0x6b, 0x61, 0x66, 0x6b, 0x61];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn nullable_string_to_wire_format_null() {
        let string = NullableString::null();
        assert_eq!(string.serialized_size(), 2);

        let bytes = string.to_wire_bytes();
        let expected = vec![0xff, 0xff];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn parse_nullable_string_null() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            NullableString::parse_bytes(&[255, 255, 4, 5, 6]),
            Ok((remaining, NullableString::null()))
        );
    }

    #[test]
    fn parse_nullable_string_empty() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            NullableString::parse_bytes(&[0, 0, 4, 5, 6]),
            Ok((remaining, NullableString::borrowed("")))
        );
    }

    #[test]
    fn parse_nullable_string() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            NullableString::parse_bytes(&[0, 3, 0x61, 0x62, 0x63, 4, 5, 6]),
            Ok((remaining, NullableString::borrowed("abc")))
        );
    }
}
