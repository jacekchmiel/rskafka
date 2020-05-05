use crate::error::ParseError;
use crate::prelude::*;
use nom::bytes::complete::take;
use nom::combinator::map;
use std::convert::TryInto;
use uuid::Uuid;

impl WireFormatParse for Uuid {
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
