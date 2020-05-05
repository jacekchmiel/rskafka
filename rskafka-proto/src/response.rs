use rskafka_wire_format::error::ParseError;
use rskafka_wire_format::prelude::*;

pub trait KafkaResponse: WireFormatParse {
    fn from_bytes(input: &[u8]) -> Result<Self, ParseError> {
        Self::from_wire_bytes(input)
    }
}
