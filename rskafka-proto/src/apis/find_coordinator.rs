use crate::{
    data::{api_key::ApiKey, error::ErrorCode, BrokerId},
    KafkaRequest, KafkaResponse,
};
use rskafka_wire_format::prelude::*;

#[derive(Debug, Clone, PartialEq, WireFormatWrite)]
pub struct FindCoordinatorRequestV2 {
    pub key: String,
    pub key_type: KeyType,
}

impl KafkaRequest for FindCoordinatorRequestV2 {
    const API_KEY: ApiKey = ApiKey::FindCoordinator;
    const API_VERSION: i16 = 2;
    type Response = FindCoordinatorResponseV2;
}

#[derive(Debug, Clone, PartialEq, WireFormatParse)]
pub struct FindCoordinatorResponseV2 {
    pub throttle_time_ms: i32,
    pub error_code: ErrorCode,
    pub error_message: NullableString<'static>,
    pub node_id: BrokerId,
    pub host: String,
    #[kafka_proto(wire_type = "i32")]
    pub port: u16,
}

impl KafkaResponse for FindCoordinatorResponseV2 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    Group = 0,
}

impl WireFormatWrite for KeyType {
    fn wire_size(&self) -> usize {
        Self::wire_size_static()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let v = *self as i8;
        v.write_into(writer)
    }
}

impl WireFormatSizeStatic for KeyType {
    fn wire_size_static() -> usize {
        i8::wire_size_static()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::hex_bytes;

    #[test]
    fn find_coordinator_request_v2() {
        let expected = hex_bytes("00096b6b6b2d746f70696300");
        let request = FindCoordinatorRequestV2 {
            key: "kkk-topic".into(),
            key_type: KeyType::Group,
        };

        assert_eq!(request.wire_size(), expected.len());
        assert_eq!(request.to_wire_bytes(), expected);
    }

    #[test]
    fn find_coordinator_response_v2() {
        let bytes = hex_bytes("000000000000ffff000003e900093132372e302e302e3100002384");
        let parsed = FindCoordinatorResponseV2::from_wire_bytes(&bytes);
        let expected = FindCoordinatorResponseV2 {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            error_message: None.into(),
            node_id: BrokerId(1001),
            host: "127.0.0.1".into(),
            port: 9092,
        };

        assert_eq!(parsed, Ok(expected))
    }
}
