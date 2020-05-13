use crate::{
    data::{api_key::ApiKey, error::ErrorCode, record::RecordBatch},
    KafkaRequest, KafkaResponse,
};
use rskafka_wire_format::{error::ParseError, prelude::*};
use std::{borrow::Cow, io::Cursor};

#[derive(Debug, Clone, PartialEq, Eq, Hash, WireFormatWrite)]
pub struct FetchRequestV4<'a> {
    pub replica_id: i32,
    pub max_wait_time: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: IsolationLevel,
    pub topics: Vec<TopicFetch<'a>>,
}

impl<'a> KafkaRequest for FetchRequestV4<'a> {
    const API_KEY: ApiKey = ApiKey::Fetch;
    const API_VERSION: i16 = 4;
    type Response = FetchResponseV4;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, WireFormatWrite)]
pub struct TopicFetch<'a> {
    pub name: Cow<'a, str>,
    pub partitions: Vec<PartitionFetch>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, WireFormatWrite)]
pub struct PartitionFetch {
    pub index: i32,
    pub fetch_offset: i64,
    pub partition_max_bytes: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IsolationLevel {
    ReadUncommitted = 0,
    ReadCommitted = 1,
}

impl IsolationLevel {
    pub fn to_i8(&self) -> i8 {
        *self as i8
    }

    pub fn from_i8(v: i8) -> Option<IsolationLevel> {
        match v {
            0 => Some(IsolationLevel::ReadUncommitted),
            1 => Some(IsolationLevel::ReadCommitted),
            _ => None,
        }
    }
}

impl WireFormatWrite for IsolationLevel {
    fn wire_size(&self) -> usize {
        i8::wire_size_static()
    }
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.to_i8().write_into(writer)
    }
}

#[derive(Debug, Clone, PartialEq, WireFormatParse)]
pub struct FetchResponseV4 {
    pub throttle_time_ms: i32,
    pub topics: Vec<FetchResponseTopic>,
}

impl KafkaResponse for FetchResponseV4 {}

#[derive(Debug, Clone, PartialEq, WireFormatParse)]
pub struct FetchResponseTopic {
    pub name: String,
    pub partitions: Vec<FetchResponsePartition>,
}

#[derive(Clone, PartialEq, WireFormatParse)]
pub struct FetchResponsePartition {
    pub index: i32,
    pub error_code: ErrorCode,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub aborted_transactions: Vec<AbortedTransaction>,
    pub record_set: Vec<u8>,
}

struct DebugLen<'a>(&'a [u8]);

impl std::fmt::Debug for DebugLen<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} bytes", self.0.len())
    }
}

impl std::fmt::Debug for FetchResponsePartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchResponsePartition")
            .field("index", &self.index)
            .field("error_code", &self.error_code)
            .field("high_watermark", &self.high_watermark)
            .field("aborted_transactions", &self.aborted_transactions)
            .field("record_set", &DebugLen(&self.record_set))
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, WireFormatParse)]
pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}
