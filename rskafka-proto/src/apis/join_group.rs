use crate::{
    data::{api_key::ApiKey, error::ErrorCode},
    KafkaRequest, KafkaResponse,
};
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq, Hash, WireFormatWrite)]
pub struct JoinGroupRequestV4<'a> {
    pub group_id: Cow<'a, str>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: Cow<'a, str>,
    pub protocol_type: Cow<'a, str>,
    pub protocols: Vec<Protocol<'a>>,
}

impl<'a> KafkaRequest for JoinGroupRequestV4<'a> {
    const API_KEY: ApiKey = ApiKey::JoinGroup;
    const API_VERSION: i16 = 4;
    type Response = JoinGroupResponseV4;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, WireFormatWrite)]
pub struct Protocol<'a> {
    pub name: Cow<'a, str>,
    pub metadata: Cow<'a, [u8]>,
}

#[derive(Debug, Clone, PartialEq, WireFormatParse)]
pub struct JoinGroupResponseV4 {
    pub throttle_time_ms: i32,
    pub error_code: ErrorCode,
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<GroupMember>,
}

impl KafkaResponse for JoinGroupResponseV4 {}

#[derive(Debug, Clone, PartialEq, WireFormatParse)]
pub struct GroupMember {
    pub member_id: String,
    pub metadata: Vec<u8>,
}
