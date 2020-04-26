use crate::proto::{
    data::{api_key::ApiKey, error::ErrorCode},
    KafkaRequest, KafkaWireFormatParse, KafkaWireFormatWrite, ParseError,
};
use either::Either;
use nom::combinator::map;
use nom::number::complete::be_i16;
use nom::sequence::tuple;

#[derive(Debug)]
pub struct ApiVersionsV0Request;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApiVersionsV0Response {
    pub error_code: ErrorCode,
    pub api_keys: Vec<ApiVersionsRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsRange {
    pub api_key: ApiKey,
    pub min_version: i16,
    pub max_version: i16,
}

impl KafkaWireFormatWrite for ApiVersionsV0Request {
    fn serialized_size(&self) -> usize {
        0
    }

    fn write_into<W: std::io::Write>(&self, _writer: &mut W) -> std::io::Result<()> {
        Ok(())
    }
}

impl KafkaRequest for ApiVersionsV0Request {
    const API_KEY: ApiKey = ApiKey::ApiVersions;
    const API_VERSION: i16 = 0;
}

struct UnknownApiKey(pub i16);

fn api_versions_filtered(input: &[u8]) -> nom::IResult<&[u8], Vec<ApiVersionsRange>, ParseError> {
    map(
        Vec::<Either<ApiVersionsRange, UnknownApiKey>>::parse_bytes,
        |versions| versions.into_iter().filter_map(Either::left).collect(),
    )(input)
}

impl<'a> KafkaWireFormatParse for ApiVersionsV0Response {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        map(
            tuple((ErrorCode::parse_bytes, api_versions_filtered)),
            |(error_code, api_keys)| ApiVersionsV0Response {
                error_code,
                api_keys,
            },
        )(input)
    }
}

impl ApiVersionsRange {
    pub fn from_tuple((api_key, min_version, max_version): (ApiKey, i16, i16)) -> Self {
        ApiVersionsRange {
            api_key,
            min_version,
            max_version,
        }
    }
}

impl KafkaWireFormatParse for Either<ApiVersionsRange, UnknownApiKey> {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        map(
            tuple((be_i16, be_i16, be_i16)),
            |(api_key_code, min_version, max_version)| match ApiKey::try_from_i16(api_key_code).ok()
            {
                Some(api_key) => Either::Left(ApiVersionsRange {
                    api_key,
                    min_version,
                    max_version,
                }),
                None => Either::Right(UnknownApiKey(api_key_code)),
            },
        )(input)
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
    use crate::proto::data::request_bytes;

    #[test]
    fn api_versions_v0_request_wire_format() {
        let bytes = request_bytes(&ApiVersionsV0Request, 1, Some("rskafka"));
        let expected = vec![
            0x00, 0x00, 0x00, 0x11, 0x00, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x07,
            0x72, 0x73, 0x6b, 0x61, 0x66, 0x6b, 0x61,
        ];

        assert_eq!(bytes, expected)
    }

    #[test]
    fn api_versions_response_v0_parse() {
        let input = vec![0, 0, 0, 0, 0, 1, 0, 18, 0, 0, 0, 7];
        let expected = ApiVersionsV0Response {
            error_code: ErrorCode(0),
            api_keys: vec![ApiVersionsRange {
                api_key: ApiKey::ApiVersions,
                min_version: 0,
                max_version: 7,
            }],
        };
        assert_eq!(ApiVersionsV0Response::from_wire_bytes(&input), Ok(expected));
    }
}
