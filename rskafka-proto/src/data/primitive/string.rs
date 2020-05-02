use crate::{
    error::{custom_error, custom_io_error},
    wire_format::*,
    ParseError,
};
use byteorder::{BigEndian, WriteBytesExt};
use nom::bytes::complete::take;
use std::borrow::Cow;
use std::convert::TryFrom;

impl KafkaWireFormatParse for String {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        let (input, size) = i16::parse_bytes(input)?;
        let size = usize::try_from(size).map_err(|_| custom_error("negative size"))?;

        let (input, bytes) = take(size)(input)?;
        let s = std::str::from_utf8(bytes).map_err(|_| custom_error("utf8 conversion failed"))?;

        Ok((input, String::from(s)))
    }
}

impl<'a> KafkaWireFormatWrite for &str {
    fn serialized_size(&self) -> usize {
        i16::serialized_size_static() + self.len()
    }
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let size = i16::try_from(self.len()).map_err(|e| custom_io_error(e))?;
        size.write_into(writer)?;
        writer.write_all(self.as_bytes())
    }
}

impl<'a> KafkaWireFormatWrite for String {
    fn serialized_size(&self) -> usize {
        self.as_str().serialized_size()
    }
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.as_str().write_into(writer)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct NullableString<'a>(pub Option<Cow<'a, str>>);

impl<'a> NullableString<'a> {
    pub fn with_borrowed(s: &'a str) -> Self {
        NullableString(Some(Cow::Borrowed(s)))
    }

    pub fn with_null() -> Self {
        NullableString(None)
    }

    pub fn into_owned_option(self) -> Option<String> {
        match self.0 {
            Some(Cow::Owned(string)) => Some(string),
            Some(Cow::Borrowed(string)) => Some(string.to_owned()),
            None => None,
        }
    }
}

impl<'a> From<Option<&'a str>> for NullableString<'a> {
    fn from(v: Option<&'a str>) -> Self {
        match v {
            Some(string) => NullableString::with_borrowed(string),
            None => NullableString::with_null(),
        }
    }
}

impl<'a> From<&'a Option<String>> for NullableString<'a> {
    fn from(v: &'a Option<String>) -> Self {
        match v {
            Some(string) => NullableString::with_borrowed(string.as_str()),
            None => NullableString::with_null(),
        }
    }
}

impl NullableString<'static> {
    pub fn with_owned(s: String) -> Self {
        NullableString(Some(Cow::Owned(s)))
    }
}

impl<'a> KafkaWireFormatWrite for NullableString<'a> {
    fn serialized_size(&self) -> usize {
        let size_size = std::mem::size_of::<i16>();
        let content_size = match self.0.as_ref() {
            None => 0,
            Some(string) => string.as_bytes().len(),
        };
        size_size + content_size
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        match self.0.as_ref() {
            None => writer.write_i16::<BigEndian>(-1),
            Some(string) => {
                let bytes = string.as_bytes();
                writer.write_i16::<BigEndian>(bytes.len() as i16)?; //TODO: conversion error
                writer.write_all(bytes)
            }
        }
    }
}

impl KafkaWireFormatParse for NullableString<'static> {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        let (input, ssize) = i16::parse_bytes(input)?;
        match ssize {
            -1 => Ok((input, NullableString::with_null())),
            other if other < 0 => panic!(), //FIXME: error
            other => {
                let count = other as usize;
                let (input, bytes) = take(count)(input)?;
                let string = std::str::from_utf8(bytes).unwrap(); // FIXME: error
                Ok((input, NullableString::with_owned(String::from(string))))
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

impl KafkaWireFormatParse for CompactNullableString<'static> {
    fn parse_bytes(_input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
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
            String::parse_bytes(&[0, 3, 0x61, 0x62, 0x63, 4, 5, 6]),
            Ok((remaining, String::from("abc")))
        );
    }

    #[test]
    fn parse_string_empty() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            String::parse_bytes(&[0, 0, 4, 5, 6]),
            Ok((remaining, String::from("")))
        );
    }

    #[test]
    fn write_string() {
        assert_eq!("abc".to_wire_bytes(), vec![0, 3, 0x61, 0x62, 0x63]);
    }

    #[test]
    fn write_string_empty() {
        assert_eq!("".to_wire_bytes(), vec![0, 0]);
    }

    #[test]
    fn nullable_string_to_wire_format() {
        let string = NullableString::with_borrowed("rskafka");
        assert_eq!(string.serialized_size(), 9);

        let bytes = string.to_wire_bytes();
        let expected = vec![0x00, 0x07, 0x72, 0x73, 0x6b, 0x61, 0x66, 0x6b, 0x61];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn nullable_string_to_wire_format_null() {
        let string = NullableString::with_null();
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
            Ok((remaining, NullableString::with_null()))
        );
    }

    #[test]
    fn parse_nullable_string_empty() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            NullableString::parse_bytes(&[0, 0, 4, 5, 6]),
            Ok((remaining, NullableString::with_borrowed("")))
        );
    }

    #[test]
    fn parse_nullable_string() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            NullableString::parse_bytes(&[0, 3, 0x61, 0x62, 0x63, 4, 5, 6]),
            Ok((remaining, NullableString::with_borrowed("abc")))
        );
    }
}
