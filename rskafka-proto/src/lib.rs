pub mod apis;
mod data;
mod error;
mod wire_format;

pub use data::{api_key::ApiKey, error::ErrorCode, BrokerId};
pub use error::ParseError;
pub use wire_format::{KafkaRequest, KafkaResponse};

#[cfg(test)]
mod test_utils {
    pub fn hex_bytes(hex_str: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        for i in 0..hex_str.len() / 2 {
            let hex_byte = &hex_str[i * 2..=i * 2 + 1];
            let byte = u8::from_str_radix(hex_byte, 16).unwrap();
            buf.push(byte);
        }

        buf
    }
}
