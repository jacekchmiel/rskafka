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
    pub use crate::{WireFormatParse, WireFormatSizeStatic, WireFormatWrite};
    pub use nom::IResult;
}

pub use nom::IResult;

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
    /// Parses bytes to create Self, borrowing data from buffer (lifetime of created object
    /// is bound to buffer lifetime). Follows nom protocol for easy parser combinations.
    fn parse_bytes(input: &[u8]) -> crate::prelude::IResult<&[u8], Self, error::ParseError>;

    /// Creates object from exact number of bytes (can return ParseError::TooMuchData)
    fn from_wire_bytes(input: &[u8]) -> Result<Self, error::ParseError> {
        match Self::parse_bytes(input) {
            Ok((&[], parsed)) => Ok(parsed),
            Ok((rem, _)) => Err(error::ParseError::TooMuchData(rem.len())),
            Err(nom::Err::Incomplete(needed)) => Err(error::ParseError::Incomplete(needed)),
            Err(nom::Err::Error(error)) | Err(nom::Err::Failure(error)) => Err(error),
        }
    }
}
