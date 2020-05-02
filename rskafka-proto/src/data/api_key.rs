use crate::{wire_format::*, ParseError};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("invalid api key value")]
pub struct InvalidApiKey;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    Metadata = 3,
    FindCoordinator = 10,
    JoinGroup = 11,
    ApiVersions = 18,
    CreateTopics = 19,
}

impl ApiKey {
    pub fn try_from_i16(v: i16) -> Result<Self, InvalidApiKey> {
        use ApiKey::*;
        match v {
            0 => Ok(Produce),
            1 => Ok(Fetch),
            3 => Ok(Metadata),
            10 => Ok(FindCoordinator),
            11 => Ok(JoinGroup),
            18 => Ok(ApiVersions),
            19 => Ok(CreateTopics),
            _ => Err(InvalidApiKey),
        }
    }

    pub fn to_i16(&self) -> i16 {
        *self as i16
    }

    pub(crate) fn serialized_size(&self) -> usize {
        std::mem::size_of_val(&self.to_i16())
    }
}

impl Into<i16> for ApiKey {
    fn into(self) -> i16 {
        self.to_i16()
    }
}

impl<'a> KafkaWireFormatParse for ApiKey {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        use nom::combinator::map_res;
        use nom::number::complete::be_i16;

        map_res(be_i16, ApiKey::try_from_i16)(input)
    }
}

impl std::fmt::Display for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}({})", self, self.to_i16())
    }
}