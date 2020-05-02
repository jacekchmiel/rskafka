use crate::{
    data::{api_key::ApiKey, error::ErrorCode},
    wire_format::*,
    ParseError,
};
use std::borrow::Cow;

pub struct JoinGroupRequestV4<'a> {
    pub group_id: Cow<'a, str>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: Cow<'a, str>,
    pub protocol_type: Cow<'a, str>,
    pub protocols: Vec<Protocol<'a>>,
}

pub struct Protocol<'a> {
    pub name: Cow<'a, str>,
    pub metadata: Cow<'a, [u8]>,
}

pub struct JoinGroupResponseV4 {
    pub throttle_time_ms: i32,
    pub error_code: ErrorCode,
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<GroupMember>,
}

pub struct GroupMember {
    pub member_id: String,
    pub metadata: Vec<u8>,
}

impl<'a> KafkaRequest for JoinGroupRequestV4<'a> {
    const API_KEY: ApiKey = ApiKey::JoinGroup;
    const API_VERSION: i16 = 4;
    type Response = JoinGroupResponseV4;
}

impl<'a> KafkaWireFormatWrite for JoinGroupRequestV4<'a> {
    fn serialized_size(&self) -> usize {
        todo!()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        todo!()
    }
}

impl KafkaResponse for JoinGroupResponseV4 {}

impl KafkaWireFormatParse for JoinGroupResponseV4 {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, ParseError> {
        todo!()
    }
}
