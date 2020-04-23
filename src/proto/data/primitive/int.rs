use crate::proto::KafkaWireFormatParse;

impl<'a> KafkaWireFormatParse<'a> for i16 {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        nom::number::complete::be_i16(input)
    }
}

impl<'a> KafkaWireFormatParse<'a> for i32 {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        nom::number::complete::be_i32(input)
    }
}

impl<'a> KafkaWireFormatParse<'a> for i64 {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        nom::number::complete::be_i64(input)
    }
}

impl<'a> KafkaWireFormatParse<'a> for u32 {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        nom::number::complete::be_u32(input)
    }
}

pub struct VarInt(pub i32);

impl<'a> KafkaWireFormatParse<'a> for VarInt {
    fn parse_bytes(_input: &'a [u8]) -> nom::IResult<&'a [u8], Self, crate::proto::ParseError> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_integers() {
        let empty: &[u8] = &[];
        assert_eq!(i16::parse_bytes(&[0, 1]).unwrap(), (empty, 1_i16));
        assert_eq!(i16::parse_bytes(&[128, 0]).unwrap(), (empty, std::i16::MIN));

        assert_eq!(i32::parse_bytes(&[0, 0, 0, 1]).unwrap(), (empty, 1_i32));
        assert_eq!(
            i32::parse_bytes(&[128, 0, 0, 0]).unwrap(),
            (empty, std::i32::MIN)
        );

        assert_eq!(
            i64::parse_bytes(&[0, 0, 0, 0, 0, 0, 0, 1]).unwrap(),
            (empty, 1_i64)
        );
        assert_eq!(
            i64::parse_bytes(&[128, 0, 0, 0, 0, 0, 0, 0]).unwrap(),
            (empty, std::i64::MIN)
        );

        assert_eq!(u32::parse_bytes(&[0, 0, 0, 1]).unwrap(), (empty, 1_u32));
        assert_eq!(
            u32::parse_bytes(&[255, 255, 255, 255]).unwrap(),
            (empty, std::u32::MAX)
        );
    }
}
