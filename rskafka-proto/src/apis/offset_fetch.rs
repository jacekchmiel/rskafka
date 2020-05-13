use crate::{
    data::{api_key::ApiKey, error::ErrorCode},
    KafkaRequest, KafkaResponse,
};
use rskafka_wire_format::prelude::*;
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq, Hash, WireFormatWrite)]
pub struct OffsetFetchRequestV1<'a> {
    pub group_id: Cow<'a, str>,
    pub topics: Vec<TopicPartitions<'a>>,
}

impl<'a> KafkaRequest for OffsetFetchRequestV1<'a> {
    const API_KEY: ApiKey = ApiKey::OffsetFetch;
    const API_VERSION: i16 = 1;
    type Response = OffsetFetchResponseV1;
}

#[derive(Debug, Clone, PartialEq, WireFormatParse)]
pub struct OffsetFetchResponseV1 {
    pub topics: Vec<TopicOffsets>,
}

impl KafkaResponse for OffsetFetchResponseV1 {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, WireFormatWrite)]
pub struct TopicPartitions<'a> {
    pub name: Cow<'a, str>,
    pub partition_indexes: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, WireFormatParse)]
pub struct TopicOffsets {
    pub name: String,
    pub partitions: Vec<PartitionOffset>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, WireFormatParse)]
pub struct PartitionOffset {
    pub index: i32,
    pub committed_offset: i64,
    pub metadata: NullableString<'static>,
    pub error_code: ErrorCode,
}
