use crate::proto::KafkaWireFormatParse;
use nom::{
    bytes::complete::take,
    combinator::{flat_map, map},
    multi::many_m_n,
    number::complete::{be_f64, be_i16, be_i32, be_i64, be_i8, be_u32},
    IResult,
};
use std::convert::TryInto;
use uuid::Uuid;

pub fn boolean(input: &[u8]) -> IResult<&[u8], bool> {
    map(be_i8, |byte| match byte {
        0 => false,
        1 => true,
        _ => panic!(), //TODO: error
    })(input)
}

pub fn int8(input: &[u8]) -> IResult<&[u8], i8> {
    be_i8(input)
}

pub fn int16(input: &[u8]) -> IResult<&[u8], i16> {
    be_i16(input)
}

pub fn int32(input: &[u8]) -> IResult<&[u8], i32> {
    be_i32(input)
}

pub fn int64(input: &[u8]) -> IResult<&[u8], i64> {
    be_i64(input)
}

pub fn uint32(input: &[u8]) -> IResult<&[u8], u32> {
    be_u32(input)
}

pub fn varint(input: &[u8]) -> IResult<&[u8], i32> {
    todo!()
}

pub fn varlong(input: &[u8]) -> IResult<&[u8], i64> {
    todo!()
}

pub fn uuid(input: &[u8]) -> IResult<&[u8], Uuid> {
    map(take(16usize), |bytes: &[u8]| {
        Uuid::from_bytes(bytes.try_into().unwrap())
    })(input)
}

pub fn float64(input: &[u8]) -> IResult<&[u8], f64> {
    be_f64(input)
}

fn try_into_usize<T: TryInto<usize> + std::fmt::Debug>(v: T) -> usize {
    //FIXME: error
    match v.try_into() {
        Ok(v) => v,
        Err(_) => panic!(),
    }
}

fn try_into_utf8(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap() //FIXME: error
}

pub fn bytes(input: &[u8]) -> IResult<&[u8], &[u8]> {
    flat_map(map(int32, try_into_usize), take)(input)
}

pub fn compact_bytes(input: &[u8]) -> IResult<&[u8], &[u8]> {
    flat_map(map(varint, |v| try_into_usize(v - 1)), take)(input)
}

pub fn nullable_bytes(input: &[u8]) -> IResult<&[u8], Option<&[u8]>> {
    let (input, ssize) = int32(input)?;
    match ssize {
        -1 => Ok((input, None)),
        other if other < 0 => panic!(), //FIXME: error
        other => {
            let count = other as usize;
            map(take(count), Some)(input)
        }
    }
}

pub fn compact_nullable_bytes(input: &[u8]) -> IResult<&[u8], Option<&[u8]>> {
    let (input, ssize) = varint(input)?;
    match ssize {
        0 => Ok((input, None)),
        other if other < 0 => panic!(), //FIXME: error
        other => {
            let count = (other - 1) as usize;
            map(take(count), Some)(input)
        }
    }
}

pub fn string(input: &[u8]) -> IResult<&[u8], &str> {
    map(flat_map(map(int16, try_into_usize), take), try_into_utf8)(input)
}

pub fn nullable_string(input: &[u8]) -> IResult<&[u8], Option<&str>> {
    let (input, ssize) = int16(input)?;
    match ssize {
        -1 => Ok((input, None)),
        other if other < 0 => panic!(), //FIXME: error
        other => {
            let count = other as usize;
            let (input, bytes) = take(count)(input)?;
            let string = try_into_utf8(bytes);
            Ok((input, Some(string)))
        }
    }
}

pub fn compact_string(input: &[u8]) -> IResult<&[u8], &str> {
    map(compact_bytes, try_into_utf8)(input)
}

pub fn compact_nullable_string(input: &[u8]) -> IResult<&[u8], Option<&str>> {
    map(compact_nullable_bytes, |v| v.map(try_into_utf8))(input)
}

pub(crate) fn array<T: KafkaWireFormatParse>(input: &[u8]) -> IResult<&[u8], Vec<T>> {
    flat_map(be_i32, |cnt| {
        let cnt = cnt as usize; //TODO: conversion error
        many_m_n(cnt, cnt, T::parse_bytes)
    })(input)
}

//TODO: array?
//TODO: compact_array?
//TODO: unsigned_varint?

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_integers() {
        let empty: &[u8] = &[];
        assert_eq!(int16(&[0, 1]).unwrap(), (empty, 1_i16));
        assert_eq!(int32(&[0, 0, 0, 1]).unwrap(), (empty, 1_i32));
        assert_eq!(int64(&[0, 0, 0, 0, 0, 0, 0, 1]).unwrap(), (empty, 1_i64));
        assert_eq!(uint32(&[0, 0, 0, 1]).unwrap(), (empty, 1_u32));
    }

    #[test]
    fn parse_uuid() {
        let empty: &[u8] = &[];
        assert_eq!(
            uuid(&[
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

    #[test]
    fn parse_bytes() {
        let expected: &[u8] = &[1, 2, 3];
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            bytes(&[0, 0, 0, 3, 1, 2, 3, 4, 5, 6]),
            Ok((remaining, expected))
        );
    }

    #[test]
    fn parse_nullable_bytes_null() {
        let expected: Option<&[u8]> = None;
        let remaining: &[u8] = &[1, 2, 3, 4, 5, 6];
        assert_eq!(
            nullable_bytes(&[255, 255, 255, 255, 1, 2, 3, 4, 5, 6]),
            Ok((remaining, expected))
        );
    }

    #[test]
    fn parse_nullable_bytes_empty() {
        let expected: Option<&[u8]> = Some(&[]);
        let remaining: &[u8] = &[1, 2, 3, 4, 5, 6];
        assert_eq!(
            nullable_bytes(&[0, 0, 0, 0, 1, 2, 3, 4, 5, 6]),
            Ok((remaining, expected))
        );
    }

    #[test]
    fn parse_nullable_bytes() {
        let expected: Option<&[u8]> = Some(&[1, 2, 3]);
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            nullable_bytes(&[0, 0, 0, 3, 1, 2, 3, 4, 5, 6]),
            Ok((remaining, expected))
        );
    }

    #[test]
    fn parse_string() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            string(&[0, 3, 0x61, 0x62, 0x63, 4, 5, 6]),
            Ok((remaining, "abc"))
        );
    }

    #[test]
    fn parse_string_empty() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(string(&[0, 0, 4, 5, 6]), Ok((remaining, "")));
    }

    #[test]
    fn parse_nullable_string_null() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(nullable_string(&[255, 255, 4, 5, 6]), Ok((remaining, None)));
    }

    #[test]
    fn parse_nullable_string_empty() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(nullable_string(&[0, 0, 4, 5, 6]), Ok((remaining, Some(""))));
    }

    #[test]
    fn parse_nullable_string() {
        let remaining: &[u8] = &[4, 5, 6];
        assert_eq!(
            nullable_string(&[0, 3, 0x61, 0x62, 0x63, 4, 5, 6]),
            Ok((remaining, Some("abc")))
        );
    }
}
