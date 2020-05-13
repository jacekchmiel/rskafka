use crate::error::{custom_error, custom_io_error, ParseError};
use crate::int::VarInt;
use crate::prelude::*;
use crate::{parse_helpers, IResult};
use byteorder::{BigEndian, WriteBytesExt};
use nom::{
    bytes::complete::take,
    combinator::{map, map_res},
};
use std::borrow::Cow;
use std::convert::TryFrom;

impl WireFormatParse for String {
    fn parse(input: &[u8]) -> IResult<&[u8], Self, ParseError> {
        let (input, size) = i16::parse(input)?;
        let size = usize::try_from(size).map_err(|_| custom_error("negative size"))?;

        let (input, bytes) = take(size)(input)?;
        let s = std::str::from_utf8(bytes).map_err(|_| custom_error("utf8 conversion failed"))?;

        Ok((input, String::from(s)))
    }
}

impl<'a> WireFormatWrite for &str {
    fn wire_size(&self) -> usize {
        i16::wire_size_static() + self.len()
    }
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let size = i16::try_from(self.len()).map_err(|e| custom_io_error(e))?;
        size.write_into(writer)?;
        writer.write_all(self.as_bytes())
    }
}

impl WireFormatWrite for String {
    fn wire_size(&self) -> usize {
        self.as_str().wire_size()
    }
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.as_str().write_into(writer)
    }
}

impl<'a> WireFormatWrite for Cow<'a, str> {
    fn wire_size(&self) -> usize {
        self.as_ref().wire_size()
    }
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.as_ref().write_into(writer)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

impl NullableString<'static> {
    pub fn with_owned(s: String) -> Self {
        NullableString(Some(Cow::Owned(s)))
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

impl<'a> From<&'a str> for NullableString<'a> {
    fn from(v: &'a str) -> Self {
        Self::with_borrowed(v)
    }
}

impl<'a> WireFormatWrite for NullableString<'a> {
    fn wire_size(&self) -> usize {
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

impl WireFormatParse for NullableString<'static> {
    fn parse(input: &[u8]) -> IResult<&[u8], Self, ParseError> {
        let (input, ssize) = i16::parse(input)?;
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
#[derive(Debug, Clone, PartialEq)]
pub struct CompactString<'a>(Cow<'a, str>);

impl<'a> CompactString<'a> {
    /// Transform to CompactString with 'static lifetime by switching wrapped data
    /// to Owned variant. Memory allocation is only performed if original data was
    /// borrowed.
    pub fn detach(self) -> CompactString<'static> {
        CompactString(self.0.into_owned().into())
    }

    /// Access wrapped str data
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl<'a> From<&'a str> for CompactString<'a> {
    fn from(s: &'a str) -> Self {
        CompactString(s.into())
    }
}

impl<'a> From<String> for CompactString<'a> {
    fn from(s: String) -> Self {
        CompactString(s.into())
    }
}

impl<'a> WireFormatWrite for CompactString<'a> {
    fn wire_size(&self) -> usize {
        let len = self.0.len();
        let len_size = VarInt(len as i32).wire_size();
        len_size + len
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        VarInt(self.0.len() as i32).write_into(writer)?; // TODO: conversion error
        writer.write_all(self.0.as_bytes())
    }
}

impl WireFormatParse for CompactString<'static> {
    fn parse(input: &[u8]) -> IResult<&[u8], Self, ParseError> {
        map(CompactString::borrow_parse, CompactString::detach)(input)
    }
}

impl<'a> WireFormatBorrowParse<'a> for CompactString<'a> {
    fn borrow_parse(input: &'a [u8]) -> IResult<&'a [u8], Self, ParseError> {
        let (input, len) = map_res(VarInt::parse, parse_helpers::int_as_usize)(input)?;
        let (input, parsed) = map_res(take(len - 1), parse_helpers::bytes_as_utf8)(input)?; //TODO underflow

        Ok((input, CompactString(parsed.into())))
    }
}

pub struct CompactNullableString<'a>(pub Option<Cow<'a, str>>);

impl WireFormatParse for CompactNullableString<'static> {
    fn parse(_input: &[u8]) -> IResult<&[u8], Self, ParseError> {
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
            String::parse(&[0, 3, 0x61, 0x62, 0x63, 4, 5, 6]),
            Ok((remaining, String::from("abc")))
        );
    }

    #[test]
    fn parse_string_empty() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            String::parse(&[0, 0, 4, 5, 6]),
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
        assert_eq!(string.wire_size(), 9);

        let bytes = string.to_wire_bytes();
        let expected = vec![0x00, 0x07, 0x72, 0x73, 0x6b, 0x61, 0x66, 0x6b, 0x61];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn nullable_string_to_wire_format_null() {
        let string = NullableString::with_null();
        assert_eq!(string.wire_size(), 2);

        let bytes = string.to_wire_bytes();
        let expected = vec![0xff, 0xff];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn parse_nullable_string_null() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            NullableString::parse(&[255, 255, 4, 5, 6]),
            Ok((remaining, NullableString::with_null()))
        );
    }

    #[test]
    fn parse_nullable_string_empty() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            NullableString::parse(&[0, 0, 4, 5, 6]),
            Ok((remaining, NullableString::with_borrowed("")))
        );
    }

    #[test]
    fn parse_nullable_string() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            NullableString::parse(&[0, 3, 0x61, 0x62, 0x63, 4, 5, 6]),
            Ok((remaining, NullableString::with_borrowed("abc")))
        );
    }
}
