use crate::proto::data::{api_key::ApiKey, error::ErrorCode, primitive::NullableString, BrokerId};
use crate::proto::{custom_error, KafkaRequest, KafkaWireFormatParse, KafkaWireFormatWrite};
use nom::sequence::tuple;
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRequestV2 {
    pub topics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataResponseV2 {
    pub brokers: Vec<BrokerMetadata>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMetadata {
    pub node_id: BrokerId,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub error: ErrorCode,
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub error: ErrorCode,
    pub partition_index: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

impl KafkaWireFormatWrite for MetadataRequestV2 {
    fn serialized_size(&self) -> usize {
        self.topics.serialized_size()
    }
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.topics.write_into(writer)
    }
}

impl KafkaRequest for MetadataRequestV2 {
    const API_KEY: ApiKey = ApiKey::Metadata;
    const API_VERSION: i16 = 2;
    type Response = MetadataResponseV2;
}

impl KafkaWireFormatParse for MetadataResponseV2 {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        let (input, brokers) = Vec::<BrokerMetadata>::parse_bytes(input)?;
        let (input, cluster_id) = NullableString::parse_bytes(input)?;
        let (input, controller_id) = i32::parse_bytes(input)?;
        let (input, topics) = Vec::<TopicMetadata>::parse_bytes(input)?;

        let metadata_response = MetadataResponseV2 {
            brokers,
            cluster_id: cluster_id.0.map(|s| s.to_string()),
            controller_id,
            topics,
        };

        Ok((input, metadata_response))
    }
}
use std::convert::TryFrom;

impl<'a> KafkaWireFormatParse for BrokerMetadata {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        let (input, node_id) = i32::parse_bytes(input)?;
        let (input, host) = String::parse_bytes(input)?;
        let (input, port) = i32::parse_bytes(input)?;
        let (input, rack) = NullableString::parse_bytes(input)?;

        let broker_metadata = BrokerMetadata {
            node_id: BrokerId(node_id),
            host,
            port: u16::try_from(port)
                .map_err(|_| custom_error("received invalid broker port value"))?,
            rack: rack.0.map(|r| r.to_string()),
        };

        Ok((input, broker_metadata))
    }
}

impl<'a> KafkaWireFormatParse for TopicMetadata {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        let (input, error) = ErrorCode::parse_bytes(input)?;
        let (input, name) = String::parse_bytes(input)?;
        let (input, is_internal) = bool::parse_bytes(input)?;
        let (input, partitions) = Vec::<PartitionMetadata>::parse_bytes(input)?;

        let topic_metadata = TopicMetadata {
            error,
            name: name.to_string(),
            is_internal,
            partitions,
        };

        Ok((input, topic_metadata))
    }
}

impl<'a> KafkaWireFormatParse for PartitionMetadata {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        let (input, error) = ErrorCode::parse_bytes(input)?;
        let (input, partition_index) = i32::parse_bytes(input)?;
        let (input, leader) = i32::parse_bytes(input)?;
        let (input, replicas) = Vec::<i32>::parse_bytes(input)?;
        let (input, isr) = Vec::<i32>::parse_bytes(input)?;

        let partition_metadata = PartitionMetadata {
            error,
            partition_index,
            leader,
            replicas,
            isr,
        };

        Ok((input, partition_metadata))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn metadata_request_v2_write() {
        // let expected_bytes = vec![
        //     0x00, 0x00, 0x00, 0x2f, 0x00, 0x03, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x07,
        //     0x72, 0x64, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x00, 0x00, 0x00, 0x01, 0x00, 0x18, 0x61,
        //     0x75, 0x74, 0x6f, 0x6d, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2d, 0x77, 0x65, 0x62, 0x68,
        //     0x6f, 0x6f, 0x6b, 0x2d, 0x69, 0x6e, 0x70, 0x75, 0x74,
        // ];
        let expected_bytes = vec![0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x61, 0x75, 0x74, 0x6f];

        let request = MetadataRequestV2 {
            topics: vec![String::from("auto")],
        };

        let bytes = request.to_wire_bytes();

        assert_eq!(bytes, expected_bytes);
    }
}
