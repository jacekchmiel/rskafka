use crate::{batch::KafkaBatch, KafkaMessage};
use rskafka_proto::{
    apis::fetch::{FetchResponsePartition, FetchResponseTopic, FetchResponseV4},
    Record, RecordBatch,
};
use rskafka_wire_format::WireFormatBorrowParse;
use std::borrow::Cow;

#[derive(Debug)]
pub struct FetchResponse {
    pub topics: Vec<FetchResponseTopic>,
}

impl FetchResponse {
    pub fn partitions<'a>(
        &'a self,
    ) -> impl Iterator<Item = (&'a str, &'a FetchResponsePartition)> + 'a {
        self.topics
            .iter()
            .flat_map(|t| t.partitions.iter().map(move |p| (t.name.as_str(), p)))
    }
}

impl From<FetchResponseV4> for FetchResponse {
    fn from(v: FetchResponseV4) -> Self {
        FetchResponse { topics: v.topics }
    }
}

impl FetchResponse {
    pub fn batches<'a>(&'a self) -> impl Iterator<Item = KafkaBatch<'a>> + 'a {
        self.topics.iter().flat_map(|t| {
            t.partitions.iter().map(move |p| {
                let batch = RecordBatch::over_wire_bytes(&p.record_set).unwrap();
                KafkaBatch::new(batch, t.name.clone(), p.index)
            })
        })
    }

    pub fn into_messages_owned(self) -> impl Iterator<Item = KafkaMessage<'static>> {
        self.topics.into_iter().flat_map(|t| {
            let topic = t.name;
            t.partitions.into_iter().flat_map(move |p| {
                let batch = RecordBatch::over_wire_bytes(&p.record_set).unwrap();
                KafkaBatch::new(batch, topic.clone(), p.index).into_messages_owned()
            })
        })
    }
}
