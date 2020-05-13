/// Utilities used for various Kafka wire-format conventions
use crate::{error::ParseError, prelude::*, VarInt};
use nom::combinator::{map_opt, map_res};
use std::convert::TryInto;

pub fn int_as_usize<T: TryInto<usize>>(v: T) -> Result<usize, ParseError> {
    v.try_into()
        .map_err(|_| ParseError::Custom("negative size".into()))
}

pub fn varint_as_opt_usize(v: VarInt) -> Result<Option<usize>, ParseError> {
    if v.0 == -1 {
        Ok(None)
    } else {
        int_as_usize(v.0).map(Some)
    }
}

pub fn bytes_as_utf8(v: &[u8]) -> Result<&str, ParseError> {
    std::str::from_utf8(v).map_err(|_| ParseError::Custom("invalid utf8 string".into()))
}

pub fn varint_encoded_size(input: &[u8]) -> nom::IResult<&[u8], usize, ParseError> {
    map_opt(map_res(VarInt::parse, int_as_usize), |s: usize| {
        s.checked_sub(1)
    })(input)
}
