#[macro_use]
extern crate rskafka_wire_format_derive;

pub mod client;
pub mod consumer;
mod error;
mod message;

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
}
