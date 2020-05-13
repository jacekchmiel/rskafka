use crate::{ApiKey, ErrorCode, KafkaRequest, KafkaResponse};
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq, Hash, WireFormatWrite)]
pub struct SyncGroupRequestV2<'a> {
    pub group_id: Cow<'a, str>,
    pub generation_id: i32,
    pub member_id: Cow<'a, str>,
    pub assignments: Vec<MemberAssignmentData<'a>>,
}

impl<'a> KafkaRequest for SyncGroupRequestV2<'a> {
    const API_KEY: ApiKey = ApiKey::SyncGroup;
    const API_VERSION: i16 = 2;
    type Response = SyncGroupResponseV2;
}

#[derive(Debug, Clone, PartialEq, WireFormatParse)]
pub struct SyncGroupResponseV2 {
    pub throttle_time_ms: i32,
    pub error_code: ErrorCode,
    pub assignment: Vec<u8>,
}

impl KafkaResponse for SyncGroupResponseV2 {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, WireFormatWrite)]
pub struct MemberAssignmentData<'a> {
    pub member_id: Cow<'a, str>,
    pub assignment: Vec<u8>,
}
