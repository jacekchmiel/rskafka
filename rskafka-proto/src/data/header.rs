use super::api_key::ApiKey;
use crate::{data::primitive::NullableString, wire_format::*, ParseError};
use byteorder::{BigEndian, WriteBytesExt};
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub(crate) struct RequestHeader<'a> {
    pub request_api_key: ApiKey,
    pub request_api_version: i16,
    pub correlation_id: i32,

    pub client_id: Option<Cow<'a, str>>,
}

impl<'a> KafkaWireFormatWrite for RequestHeader<'a> {
    fn serialized_size(&self) -> usize {
        self.request_api_key.serialized_size()
            + self.request_api_version.serialized_size()
            + self.correlation_id.serialized_size()
            + NullableString(self.client_id.clone()).serialized_size()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i16::<BigEndian>(self.request_api_key.to_i16())?;
        writer.write_i16::<BigEndian>(self.request_api_version)?;
        writer.write_i32::<BigEndian>(self.correlation_id)?;
        NullableString(self.client_id.clone()).write_into(writer)?;

        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct ResponseHeader {
    pub correlation_id: i32,
}

impl<'a> KafkaWireFormatParse for ResponseHeader {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        use nom::combinator::map;
        use nom::number::complete::be_i32;

        map(be_i32, |correlation_id| ResponseHeader { correlation_id })(input)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn header_wire_format() {
        let header = RequestHeader {
            request_api_key: ApiKey::ApiVersions,
            request_api_version: 0,
            correlation_id: 1,
            client_id: Some(Cow::Borrowed("rskafka")),
        };

        assert_eq!(header.serialized_size(), 17);

        let bytes = header.to_wire_bytes();
        let expected = vec![
            0x00, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x07, 0x72, 0x73, 0x6b, 0x61,
            0x66, 0x6b, 0x61,
        ];
        assert_eq!(bytes, expected);
    }
}
