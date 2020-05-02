use crate::proto::{
    data::{api_key::ApiKey, error::ErrorCode, primitive::NullableString, BrokerId},
    KafkaRequest, KafkaWireFormatParse, KafkaWireFormatStaticSize, KafkaWireFormatWrite,
};
#[derive(Debug, Clone, PartialEq)]
pub struct FindCoordinatorRequestV2 {
    pub key: String,
    pub key_type: KeyType,
}
#[derive(Debug, Clone, PartialEq)]
pub struct FindCoordinatorResponseV2 {
    pub throttle_time_ms: i32,
    pub error_code: ErrorCode,
    pub error_message: Option<String>,
    pub node_id: BrokerId,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    Group = 0,
}

impl KafkaWireFormatWrite for KeyType {
    fn serialized_size(&self) -> usize {
        Self::serialized_size_static()
    }
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let v = *self as i8;
        v.write_into(writer)
    }
}

impl KafkaWireFormatStaticSize for KeyType {
    fn serialized_size_static() -> usize {
        i8::serialized_size_static()
    }
}

impl KafkaRequest for FindCoordinatorRequestV2 {
    const API_KEY: ApiKey = ApiKey::FindCoordinator;
    const API_VERSION: i16 = 2;
    type Response = FindCoordinatorResponseV2;
}

impl KafkaWireFormatWrite for FindCoordinatorRequestV2 {
    fn serialized_size(&self) -> usize {
        self.key.serialized_size() + self.key_type.serialized_size()
    }
    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.key.write_into(writer)?;
        self.key_type.write_into(writer)
    }
}

impl KafkaWireFormatParse for FindCoordinatorResponseV2 {
    fn parse_bytes(input: &[u8]) -> nom::IResult<&[u8], Self, crate::proto::ParseError> {
        let (input, throttle_time_ms) = i32::parse_bytes(input)?;
        let (input, error_code) = ErrorCode::parse_bytes(input)?;
        let (input, error_message) = NullableString::parse_bytes(input)?;
        let (input, node_id) = BrokerId::parse_bytes(input)?;
        let (input, host) = String::parse_bytes(input)?;
        let (input, port) = i32::parse_bytes(input)?;

        let response = FindCoordinatorResponseV2 {
            throttle_time_ms,
            error_code,
            error_message: error_message.into_owned_option(),
            node_id,
            host,
            port: port as u16,
        };

        Ok((input, response))
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

        assert_eq!(request.serialized_size(), expected.len());
        assert_eq!(request.to_wire_bytes(), expected);
    }

    #[test]
    fn find_coordinator_response_v2() {
        let bytes = hex_bytes("000000000000ffff000003e900093132372e302e302e3100002384");
        let parsed = FindCoordinatorResponseV2::from_wire_bytes(&bytes);
        let expected = FindCoordinatorResponseV2 {
            throttle_time_ms: 0,
            error_code: ErrorCode(0),
            error_message: None,
            node_id: BrokerId(1001),
            host: "127.0.0.1".into(),
            port: 9092,
        };

        assert_eq!(parsed, Ok(expected))
    }
}
