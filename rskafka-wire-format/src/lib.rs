mod array;
mod boolean;
mod bytes;
pub mod error;
mod int;
mod string;
mod tuple;
mod uuid;

pub mod prelude {
    pub use crate::string::NullableString;
    pub use crate::{
        WireFormatBorrowParse, WireFormatParse, WireFormatSizeStatic, WireFormatWrite,
    };
    pub use nom::IResult;
}

pub use bytes::CompactBytes;
pub use int::VarInt;
pub use nom::IResult;
pub use string::CompactString;
pub mod parse_helpers;

/// Object that can be written in Kafka Protocol wire format
pub trait WireFormatWrite {
    fn wire_size(&self) -> usize;
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()>;

    /// Writes data to new byte vector
    fn to_wire_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.wire_size());
        self.write_into(&mut buffer)
            .expect("write into buffer failed");
        buffer
    }
}

pub trait WireFormatSizeStatic {
    fn wire_size_static() -> usize;
}

/// Object that can be parsed from Kafka Protocol wire data
pub trait WireFormatParse: Sized {
    //TODO: rename to just parse
    /// Parses bytes to create Self. Follows nom protocol for easy parser combinations.
    fn parse(input: &[u8]) -> crate::prelude::IResult<&[u8], Self, error::ParseError>;

    /// Creates object from exact number of bytes (can return ParseError::TooMuchData)
    fn from_wire_bytes(input: &[u8]) -> Result<Self, error::ParseError> {
        match Self::parse(input) {
            Ok((&[], parsed)) => Ok(parsed),
            Ok((rem, _)) => Err(error::ParseError::TooMuchData(rem.len())),
            Err(nom::Err::Incomplete(needed)) => Err(error::ParseError::Incomplete(needed)),
            Err(nom::Err::Error(error)) | Err(nom::Err::Failure(error)) => Err(error),
        }
    }
}

pub trait WireFormatBorrowParse<'a>: Sized {
    /// Parses bytes to create Self, borrowing data from buffer (lifetime of created object
    /// is bound to buffer lifetime). Follows nom protocol for easy parser combinations.
    fn borrow_parse(input: &'a [u8]) -> crate::prelude::IResult<&'a [u8], Self, error::ParseError>;

    /// Creates object from exact number of bytes (can return ParseError::TooMuchData)
    fn over_wire_bytes(input: &'a [u8]) -> Result<Self, error::ParseError> {
        match Self::borrow_parse(input) {
            Ok((&[], parsed)) => Ok(parsed),
            Ok((rem, _)) => Err(error::ParseError::TooMuchData(rem.len())),
            Err(nom::Err::Incomplete(needed)) => Err(error::ParseError::Incomplete(needed)),
            Err(nom::Err::Error(error)) | Err(nom::Err::Failure(error)) => Err(error),
        }
    }
}
