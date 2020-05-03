use super::api_key::ApiKey;
use crate::data::primitive::NullableString;

#[derive(Debug, Clone, WireFormatWrite)]
pub(crate) struct RequestHeader<'a> {
    pub request_api_key: ApiKey,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: NullableString<'a>,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::wire_format::*;

    #[test]
    fn header_wire_format() {
        let header = RequestHeader {
            request_api_key: ApiKey::ApiVersions,
            request_api_version: 0,
            correlation_id: 1,
            client_id: "rskafka".into(),
        };

        assert_eq!(header.wire_size(), 17);

        let bytes = header.to_wire_bytes();
        let expected = vec![
            0x00, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x07, 0x72, 0x73, 0x6b, 0x61,
            0x66, 0x6b, 0x61,
        ];
        assert_eq!(bytes, expected);
    }
}
