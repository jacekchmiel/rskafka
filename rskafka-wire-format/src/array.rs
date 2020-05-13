use crate::error::{custom_error, ParseError};
use crate::{parse_helpers, prelude::*};
use nom::{
    combinator::{iterator, map, map_res},
    multi::many_m_n,
};
use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
};

impl<T> WireFormatParse for Vec<T>
where
    T: WireFormatParse,
{
    fn parse(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        let (input, size) = i32::parse(input)?;

        // Seems like each array might sometimes be a "null array" encoded with lenghth set to -1.
        // Let's keep interface simple and use empty vec in that case until we need to distinguish
        // empty and "null array".
        if size == -1 {
            Ok((input, vec![]))
        } else {
            let size = usize::try_from(size).map_err(|_| custom_error("negative size"))?;
            many_m_n(size, size, T::parse)(input)
        }
    }
}

impl<'a, T: WireFormatBorrowParse<'a>> WireFormatBorrowParse<'a> for Vec<T> {
    fn borrow_parse(input: &'a [u8]) -> IResult<&'a [u8], Self, ParseError> {
        let (input, size) = map_res(i32::parse, parse_helpers::int_as_usize)(input)?;
        let mut it = iterator(input, T::borrow_parse);
        let items: Vec<T> = it.collect();
        let input = it.finish().map(|(input, ())| input)?;

        if items.len() != size {
            return Err(custom_error("invalid number of items"));
        } else {
            Ok((input, items))
        }
    }
}

impl<T> WireFormatParse for Cow<'static, [T]>
where
    T: WireFormatParse + Clone,
{
    fn parse(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        map(Vec::<T>::parse, Cow::Owned)(input)
    }
}

impl<T> WireFormatWrite for [T]
where
    T: WireFormatWrite,
{
    fn wire_size(&self) -> usize {
        let len_size = i32::wire_size_static();
        let fields_size: usize = self.iter().map(|v| v.wire_size()).sum::<usize>();

        len_size + fields_size
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        i32::try_from(self.len())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
            .write_into(writer)?;

        for item in self.iter() {
            item.write_into(writer)?;
        }

        Ok(())
    }
}

impl<'a, T> WireFormatWrite for Cow<'a, [T]>
where
    T: WireFormatWrite + Clone,
{
    fn wire_size(&self) -> usize {
        self.as_ref().wire_size()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.as_ref().write_into(writer)
    }
}

impl<T> WireFormatWrite for Vec<T>
where
    T: WireFormatWrite,
{
    fn wire_size(&self) -> usize {
        self.as_slice().wire_size()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.as_slice().write_into(writer)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn array_write() {
        let array = vec![1i32, 2i32];
        let expected = vec![0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2];

        assert_eq!(array.wire_size(), expected.len());
        assert_eq!(array.to_wire_bytes(), expected);
    }
}
