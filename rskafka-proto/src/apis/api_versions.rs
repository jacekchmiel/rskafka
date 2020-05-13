use crate::{
    data::{api_key::ApiKey, error::ErrorCode},
    KafkaRequest, KafkaResponse,
};
// use nom::combinator::map;
// use nom::sequence::tuple;
use rskafka_wire_format::{error::ParseError, prelude::*};

#[derive(Debug, Clone, Copy, PartialEq, Eq, WireFormatWrite)]
pub struct ApiVersionsRequestV0;

impl KafkaRequest for ApiVersionsRequestV0 {
    const API_KEY: ApiKey = ApiKey::ApiVersions;
    const API_VERSION: i16 = 0;
    type Response = ApiVersionsResponseV0;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsResponseV0 {
    pub error_code: ErrorCode,
    pub api_keys: Vec<ApiVersionsRange>,
}

impl KafkaResponse for ApiVersionsResponseV0 {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsRange {
    pub api_key: ApiKey,
    pub min_version: i16,
    pub max_version: i16,
}

impl ApiVersionsRange {
    fn try_from_raw(v: ApiVersionsRangeRaw) -> Option<Self> {
        ApiKey::try_from_i16(v.api_key).map(|api_key| ApiVersionsRange {
            api_key,
            min_version: v.min_version,
            max_version: v.max_version,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, WireFormatParse)]
struct ApiVersionsRangeRaw {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

impl WireFormatParse for ApiVersionsResponseV0 {
    fn parse(input: &[u8]) -> IResult<&[u8], Self, ParseError> {
        let (input, error_code) = ErrorCode::parse(input)?;
        let (input, raw) = Vec::<ApiVersionsRangeRaw>::parse(input)?;
        let api_keys = raw
            .into_iter()
            .filter_map(ApiVersionsRange::try_from_raw)
            .collect();
        let response = ApiVersionsResponseV0 {
            error_code,
            api_keys,
        };

        Ok((input, response))
    }
}

impl std::fmt::Display for ApiVersionsRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {}-{}",
            self.api_key, self.min_version, self.max_version
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn api_versions_v0_request_wire_format() {
        let bytes = ApiVersionsRequestV0.to_bytes(1, Some("rskafka"));
        let expected = vec![
            0x00, 0x00, 0x00, 0x11, 0x00, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x07,
            0x72, 0x73, 0x6b, 0x61, 0x66, 0x6b, 0x61,
        ];

        assert_eq!(bytes, expected)
    }

    #[test]
    fn api_versions_response_v0_parse() {
        let input = vec![0, 0, 0, 0, 0, 1, 0, 18, 0, 0, 0, 7];
        let expected = ApiVersionsResponseV0 {
            error_code: ErrorCode::None,
            api_keys: vec![ApiVersionsRange {
                api_key: ApiKey::ApiVersions,
                min_version: 0,
                max_version: 7,
            }],
        };
        assert_eq!(ApiVersionsResponseV0::from_wire_bytes(&input), Ok(expected));
    }
}
