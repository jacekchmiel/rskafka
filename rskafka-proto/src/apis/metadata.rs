use crate::data::{api_key::ApiKey, error::ErrorCode, primitive::NullableString, BrokerId};
use crate::wire_format::*;

#[derive(Debug, Clone, PartialEq, Eq, WireFormatWrite)]
pub struct MetadataRequestV2 {
    pub topics: Vec<String>,
}

impl KafkaRequest for MetadataRequestV2 {
    const API_KEY: ApiKey = ApiKey::Metadata;
    const API_VERSION: i16 = 2;
    type Response = MetadataResponseV2;
}

#[derive(Debug, Clone, PartialEq, Eq, WireFormatParse, KafkaResponse)]
pub struct MetadataResponseV2 {
    pub brokers: Vec<BrokerMetadata>,
    pub cluster_id: NullableString<'static>,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, WireFormatParse)]
pub struct BrokerMetadata {
    pub node_id: BrokerId,
    pub host: String,
    #[kafka_proto(wire_type = "i32")]
    pub port: u16,
    pub rack: NullableString<'static>,
}

#[derive(Debug, Clone, PartialEq, Eq, WireFormatParse)]
pub struct TopicMetadata {
    pub error: ErrorCode,
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, WireFormatParse)]
pub struct PartitionMetadata {
    pub error: ErrorCode,
    pub partition_index: i32,
    pub leader: BrokerId,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
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
