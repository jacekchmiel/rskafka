use crate::proto::{
    data::{api_key::ApiKey, error::ErrorCode, primitive::NullableString},
    KafkaRequest, KafkaWireFormatParse, KafkaWireFormatWrite,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CreateTopic {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub assignments: Vec<TopicAssignment>,
    pub configs: Vec<TopicConfig>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TopicAssignment {
    partition: i32,
    broker_ids: Vec<i32>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TopicConfig {
    name: String,
    value: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateTopicsRequestV1 {
    pub topics: Vec<CreateTopic>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateTopicsResponseV1 {
    pub topics: Vec<CreateTopicResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateTopicResponse {
    pub name: String,
    pub error_code: ErrorCode,
    pub error_message: Option<String>,
}

impl KafkaWireFormatWrite for CreateTopicsRequestV1 {
    fn serialized_size(&self) -> usize {
        self.topics.serialized_size()
            + self.timeout_ms.serialized_size()
            + self.validate_only.serialized_size()
    }
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.topics.write_into(writer)?;
        self.timeout_ms.write_into(writer)?;
        self.validate_only.write_into(writer)
    }
}

impl KafkaWireFormatWrite for CreateTopic {
    fn serialized_size(&self) -> usize {
        self.name.serialized_size()
            + self.partitions.serialized_size()
            + self.replication_factor.serialized_size()
            + self.assignments.serialized_size()
            + self.configs.serialized_size()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.name.write_into(writer)?;
        self.partitions.write_into(writer)?;
        self.replication_factor.write_into(writer)?;
        self.assignments.write_into(writer)?;
        self.configs.write_into(writer)
    }
}

impl KafkaWireFormatWrite for TopicAssignment {
    fn serialized_size(&self) -> usize {
        self.partition.serialized_size() + self.broker_ids.serialized_size()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.partition.write_into(writer)?;
        self.broker_ids.write_into(writer)
    }
}

impl KafkaWireFormatWrite for TopicConfig {
    fn serialized_size(&self) -> usize {
        self.name.serialized_size() + NullableString::from(&self.value).serialized_size()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.name.write_into(writer)?;
        NullableString::from(&self.value).write_into(writer)
    }
}

impl KafkaRequest for CreateTopicsRequestV1 {
    const API_KEY: ApiKey = ApiKey::CreateTopics;
    const API_VERSION: i16 = 1;
    type Response = CreateTopicsResponseV1;
}

impl KafkaWireFormatParse for CreateTopicsResponseV1 {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        let (input, topics) = KafkaWireFormatParse::parse_bytes(input)?;

        Ok((input, CreateTopicsResponseV1 { topics }))
    }
}

impl KafkaWireFormatParse for CreateTopicResponse {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        use nom::sequence::tuple;
        let (input, fields) = tuple((
            String::parse_bytes,
            ErrorCode::parse_bytes,
            NullableString::parse_bytes,
        ))(input)?;

        let response = CreateTopicResponse {
            name: fields.0,
            error_code: fields.1,
            error_message: fields.2.into_owned_option(),
        };

        Ok((input, response))
    }
}
