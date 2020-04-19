use crate::proto::{
    data::{api_key::ApiKey, error::ErrorCode},
    decode::primitive::array,
    KafkaRequest, KafkaWireFormatParse, KafkaWireFormatWrite,
};
use either::Either;
use nom::combinator::map;
use nom::number::complete::be_i16;
use nom::sequence::tuple;

#[derive(Debug)]
pub struct ApiVersionsV0Request;

impl KafkaWireFormatWrite for ApiVersionsV0Request {
    fn serialized_size(&self) -> usize {
        0
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<usize> {
        Ok(0)
    }
}

impl KafkaRequest for ApiVersionsV0Request {
    const API_KEY: ApiKey = ApiKey::ApiVersions;
    const API_VERSION: i16 = 0;
}

// ApiVersions Response (Version: 0) => error_code [api_keys]
//   error_code => INT16
//   api_keys => api_key min_version max_version
//     api_key => INT16
//     min_version => INT16
//     max_version => INT16
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApiVersionsV0Response {
    pub error: Option<ErrorCode>,
    pub api_keys: Vec<ApiVersionsRange>,
}

impl ApiVersionsV0Response {
    pub fn from_tuple((error, api_keys): (Option<ErrorCode>, Vec<ApiVersionsRange>)) -> Self {
        ApiVersionsV0Response { error, api_keys }
    }
}

fn api_versions_filtered(input: &[u8]) -> nom::IResult<&[u8], Vec<ApiVersionsRange>> {
    map(
        array::<Either<ApiVersionsRange, UnknownApiKey>>,
        |versions| versions.into_iter().filter_map(Either::left).collect(),
    )(input)
}

impl KafkaWireFormatParse for ApiVersionsV0Response {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self> {
        map(
            tuple((Option::<ErrorCode>::parse_bytes, api_versions_filtered)),
            ApiVersionsV0Response::from_tuple,
        )(input)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsRange {
    pub api_key: ApiKey,
    pub min_version: i16,
    pub max_version: i16,
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

pub struct UnknownApiKey(pub i16);

impl KafkaWireFormatParse for Either<ApiVersionsRange, UnknownApiKey> {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self> {
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
            error: None,
            api_keys: vec![ApiVersionsRange {
                api_key: ApiKey::ApiVersions,
                min_version: 0,
                max_version: 7,
            }],
        };
        assert_eq!(ApiVersionsV0Response::from_wire_bytes(&input), Ok(expected));
    }
}
