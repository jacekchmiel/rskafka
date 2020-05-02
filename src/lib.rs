pub mod client;
pub mod consumer;
mod error;
mod message;
mod proto;

pub use consumer::{Consumer, ConsumerConfig};
pub use error::Error;
pub use message::{KafkaMessage, KafkaOffset};

#[cfg(test)]
mod test_utils {
    pub fn init_logger() {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init()
            .ok();
    }

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
