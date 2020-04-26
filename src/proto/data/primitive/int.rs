use crate::proto::{KafkaWireFormatParse, KafkaWireFormatStaticSize, KafkaWireFormatWrite};
use byteorder::{BigEndian, WriteBytesExt};
use std::io::Write;

impl KafkaWireFormatParse for i16 {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        nom::number::complete::be_i16(input)
    }
}

impl KafkaWireFormatWrite for i16 {
    fn serialized_size(&self) -> usize {
        Self::serialized_size_static()
    }

    fn write_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i16::<BigEndian>(*self)
    }
}

impl KafkaWireFormatStaticSize for i16 {
    fn serialized_size_static() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl<'a> KafkaWireFormatParse for i32 {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        nom::number::complete::be_i32(input)
    }
}

impl KafkaWireFormatWrite for i32 {
    fn serialized_size(&self) -> usize {
        Self::serialized_size_static()
    }

    fn write_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i32::<BigEndian>(*self)
    }
}

impl KafkaWireFormatStaticSize for i32 {
    fn serialized_size_static() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl<'a> KafkaWireFormatParse for i64 {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        nom::number::complete::be_i64(input)
    }
}

impl KafkaWireFormatWrite for i64 {
    fn serialized_size(&self) -> usize {
        Self::serialized_size_static()
    }

    fn write_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i64::<BigEndian>(*self)
    }
}

impl KafkaWireFormatStaticSize for i64 {
    fn serialized_size_static() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl KafkaWireFormatParse for u32 {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        nom::number::complete::be_u32(input)
    }
}

impl KafkaWireFormatWrite for u32 {
    fn serialized_size(&self) -> usize {
        Self::serialized_size_static()
    }

    fn write_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32::<BigEndian>(*self)
    }
}

impl KafkaWireFormatStaticSize for u32 {
    fn serialized_size_static() -> usize {
        std::mem::size_of::<Self>()
    }
}

pub struct VarInt(pub i32);

impl KafkaWireFormatParse for VarInt {
    fn parse_bytes(_input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
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
