use std::{borrow::Cow, fmt};

#[derive(Debug)]
pub struct KafkaMessage<'a> {
    pub topic: Cow<'a, str>,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Cow<'a, [u8]>>,
    pub value: Option<Cow<'a, [u8]>>,
}

impl<'a> KafkaMessage<'a> {
    pub fn into_offset(self) -> KafkaOffset<'a> {
        KafkaOffset {
            topic: self.topic,
            partition: self.partition,
            offset: self.offset,
        }
    }

    pub fn detached(&self) -> KafkaMessage<'static> {
        KafkaMessage {
            topic: self.topic.as_ref().to_owned().into(),
            partition: self.partition,
            offset: self.offset,
            key: self.key.as_ref().map(|k| Cow::Owned(k.as_ref().to_owned())),
            value: self
                .value
                .as_ref()
                .map(|k| Cow::Owned(k.as_ref().to_owned())),
        }
    }
}

pub struct KafkaOffset<'a> {
    pub topic: Cow<'a, str>,
    pub partition: i32,
    pub offset: i64,
}

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
