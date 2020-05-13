use crate::error::{custom_error, ParseError};
use crate::prelude::*;
use byteorder::{BigEndian, WriteBytesExt};
use integer_encoding::{VarIntReader, VarIntWriter};
use std::{convert::TryInto, io::Write};

impl WireFormatParse for i8 {
    fn parse(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        nom::number::complete::be_i8(input)
    }
}

impl WireFormatWrite for i8 {
    fn wire_size(&self) -> usize {
        Self::wire_size_static()
    }

    fn write_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i8(*self)
    }
}

impl WireFormatSizeStatic for i8 {
    fn wire_size_static() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl WireFormatParse for i16 {
    fn parse(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        nom::number::complete::be_i16(input)
    }
}

impl WireFormatWrite for i16 {
    fn wire_size(&self) -> usize {
        Self::wire_size_static()
    }

    fn write_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i16::<BigEndian>(*self)
    }
}

impl WireFormatSizeStatic for i16 {
    fn wire_size_static() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl<'a> WireFormatParse for i32 {
    fn parse(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        nom::number::complete::be_i32(input)
    }
}

impl WireFormatWrite for i32 {
    fn wire_size(&self) -> usize {
        Self::wire_size_static()
    }

    fn write_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i32::<BigEndian>(*self)
    }
}

impl WireFormatSizeStatic for i32 {
    fn wire_size_static() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl<'a> WireFormatParse for i64 {
    fn parse(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        nom::number::complete::be_i64(input)
    }
}

impl WireFormatWrite for i64 {
    fn wire_size(&self) -> usize {
        Self::wire_size_static()
    }

    fn write_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i64::<BigEndian>(*self)
    }
}

impl WireFormatSizeStatic for i64 {
    fn wire_size_static() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl WireFormatParse for u32 {
    fn parse(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        nom::number::complete::be_u32(input)
    }
}

impl WireFormatWrite for u32 {
    fn wire_size(&self) -> usize {
        Self::wire_size_static()
    }

    fn write_into<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32::<BigEndian>(*self)
    }
}

impl WireFormatSizeStatic for u32 {
    fn wire_size_static() -> usize {
        std::mem::size_of::<Self>()
    }
}

#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct VarInt(pub i32);

impl WireFormatParse for VarInt {
    fn parse(mut input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        let v: i32 = input
            .read_varint()
            .map_err(|_| custom_error("varint parse failed"))?;

        Ok((input, VarInt(v)))
    }
}

impl WireFormatWrite for VarInt {
    fn wire_size(&self) -> usize {
        use integer_encoding::VarInt as VarIntTrait;
        self.0.required_space()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_varint(self.0).map(|_| ())
    }
}

impl TryInto<usize> for VarInt {
    type Error = std::num::TryFromIntError;
    fn try_into(self) -> Result<usize, Self::Error> {
        self.0.try_into()
    }
}

impl Into<i32> for VarInt {
    fn into(self) -> i32 {
        self.0
    }
}

impl From<i32> for VarInt {
    fn from(v: i32) -> Self {
        VarInt(v)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_integers() {
        let empty: &[u8] = &[];
        assert_eq!(i16::parse(&[0, 1]).unwrap(), (empty, 1_i16));
        assert_eq!(i16::parse(&[128, 0]).unwrap(), (empty, std::i16::MIN));

        assert_eq!(i32::parse(&[0, 0, 0, 1]).unwrap(), (empty, 1_i32));
        assert_eq!(i32::parse(&[128, 0, 0, 0]).unwrap(), (empty, std::i32::MIN));

        assert_eq!(
            i64::parse(&[0, 0, 0, 0, 0, 0, 0, 1]).unwrap(),
            (empty, 1_i64)
        );
        assert_eq!(
            i64::parse(&[128, 0, 0, 0, 0, 0, 0, 0]).unwrap(),
            (empty, std::i64::MIN)
        );

        assert_eq!(u32::parse(&[0, 0, 0, 1]).unwrap(), (empty, 1_u32));
        assert_eq!(
            u32::parse(&[255, 255, 255, 255]).unwrap(),
            (empty, std::u32::MAX)
        );
    }

    #[test]
    fn parse_varint() {
        assert_eq!(VarInt::from_wire_bytes(&[0x01]).unwrap(), VarInt(-1));
        assert_eq!(VarInt::from_wire_bytes(&[0x0c]).unwrap(), VarInt(6));
        assert_eq!(VarInt::from_wire_bytes(&[0x18]).unwrap(), VarInt(12));
        assert_eq!(VarInt::from_wire_bytes(&[0xd8, 0x04]).unwrap(), VarInt(300));
    }

    #[test]
    fn encode_varint() {
        assert_eq!(VarInt(-1).to_wire_bytes(), &[0x01]);
        assert_eq!(VarInt(6).to_wire_bytes(), &[0x0c]);
        assert_eq!(VarInt(12).to_wire_bytes(), &[0x18]);
        assert_eq!(VarInt(300).to_wire_bytes(), &[0xd8, 0x04]);
    }
}
