use std::fmt;

#[derive(Debug)]
pub struct KafkaMessage;

impl KafkaMessage {
    pub fn into_offset(self) -> KafkaOffset {
        todo!()
    }
}
pub struct KafkaOffset;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaPartition {
    pub topic_name: String,
    pub partition_index: i32,
}

impl fmt::Display for KafkaPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[{}]", self.topic_name, self.partition_index)
    }
}
