use crate::{
    data::{api_key::ApiKey, error::ErrorCode, primitive::NullableString},
    wire_format::*,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash, KafkaWireFormatWrite)]
pub struct CreateTopic {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub assignments: Vec<TopicAssignment>,
    pub configs: Vec<TopicConfig>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, KafkaWireFormatWrite)]
pub struct TopicAssignment {
    partition: i32,
    broker_ids: Vec<i32>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, KafkaWireFormatWrite)]
pub struct TopicConfig {
    name: String,
    value: NullableString<'static>,
}

impl CreateTopic {
    pub fn with_name<S: AsRef<str>>(value: S) -> Self {
        CreateTopic {
            name: value.as_ref().to_string(),
            partitions: 1,
            replication_factor: 1,
            assignments: Vec::new(),
            configs: Vec::new(),
        }
    }

    pub fn partitions(mut self, value: i32) -> Self {
        self.partitions = value;

        self
    }

    pub fn replication_factor(mut self, value: i16) -> Self {
        self.replication_factor = value;

        self
    }

    //TODO: assignments
    //TODO: configs
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, KafkaWireFormatWrite)]
pub struct CreateTopicsRequestV1 {
    pub topics: Vec<CreateTopic>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, KafkaWireFormatParse, KafkaResponse)]
pub struct CreateTopicsResponseV1 {
    pub topics: Vec<CreateTopicResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, KafkaWireFormatParse)]
pub struct CreateTopicResponse {
    pub name: String,
    pub error_code: ErrorCode,
    pub error_message: NullableString<'static>,
}

impl KafkaRequest for CreateTopicsRequestV1 {
    const API_KEY: ApiKey = ApiKey::CreateTopics;
    const API_VERSION: i16 = 1;
    type Response = CreateTopicsResponseV1;
}
