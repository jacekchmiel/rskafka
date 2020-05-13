use crate::KafkaMessage;
use rskafka_proto::{Record, RecordBatch};
use std::borrow::Cow;

pub struct KafkaBatch<'a> {
    pub topic: String,
    pub partition_index: i32,
    pub base_offset: i64,
    pub records: Vec<KafkaBatchRecord<'a>>,
}

impl<'a> KafkaBatch<'a> {
    pub(crate) fn new(data: RecordBatch<'a>, topic: String, partition_index: i32) -> Self {
        KafkaBatch {
            topic,
            partition_index,
            base_offset: data.base_offset,
            records: data
                .records
                .into_owned()
                .into_iter()
                .map(KafkaBatchRecord::from)
                .collect(),
        }
    }

    pub fn messages(&'a self) -> impl Iterator<Item = KafkaMessage<'a>> + 'a {
        self.records.iter().map(move |r| KafkaMessage {
            topic: Cow::Borrowed(&self.topic),
            partition: self.partition_index,
            offset: self.base_offset + r.offset_delta as i64,
            key: r.key.as_ref().map(|key| Cow::Borrowed(key.as_ref())),
            value: r.value.as_ref().map(|value| Cow::Borrowed(value.as_ref())),
        })
    }

    pub fn into_messages_owned(self) -> Vec<KafkaMessage<'static>> {
        let topic = self.topic;
        let partition_index = self.partition_index;
        let base_offset = self.base_offset;
        self.records
            .into_iter()
            .map(move |r| KafkaMessage {
                topic: topic.clone().into(),
                partition: partition_index,
                offset: base_offset + r.offset_delta as i64,
                key: r.key.map(|k| k.into_owned().into()),
                value: r.value.map(|v| v.into_owned().into()),
            })
            .collect()
    }
}

pub struct KafkaBatchRecord<'a> {
    pub offset_delta: i32,
    pub key: Option<Cow<'a, [u8]>>,
    pub value: Option<Cow<'a, [u8]>>,
}

impl<'a> From<Record<'a>> for KafkaBatchRecord<'a> {
    fn from(v: Record<'a>) -> Self {
        KafkaBatchRecord {
            offset_delta: v.offset_delta.0,
            key: v.key,
            value: v.value,
        }
    }
}
