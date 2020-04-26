use thiserror::Error;
#[derive(Debug, Error)]
#[error("kafka error, error_code={0}")]
pub struct KafkaError(pub(crate) i16);
