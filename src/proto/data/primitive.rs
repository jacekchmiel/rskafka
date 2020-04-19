use super::KafkaWireFormatWrite;
use byteorder::{BigEndian, WriteBytesExt};

pub(crate) struct NullableString<'a>(pub Option<&'a str>);

impl KafkaWireFormatWrite for NullableString<'_> {
    fn serialized_size(&self) -> usize {
        let size_size = std::mem::size_of::<i16>();
        let content_size = match self.0 {
            None => 0,
            Some(string) => string.as_bytes().len(),
        };
        size_size + content_size
    }
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<usize> {
        match self.0 {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn nullable_string_wire_format() {
        let string = NullableString(Some("rskafka"));
        assert_eq!(string.serialized_size(), 9);

        let bytes = string.to_wire_bytes();
        let expected = vec![0x00, 0x07, 0x72, 0x73, 0x6b, 0x61, 0x66, 0x6b, 0x61];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn nullable_string_wire_format_null() {
        let string = NullableString(None);
        assert_eq!(string.serialized_size(), 2);

        let bytes = string.to_wire_bytes();
        let expected = vec![0xff, 0xff];
        assert_eq!(bytes, expected);
    }
}
