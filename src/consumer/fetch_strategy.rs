use super::fetch_data::FetchResponse;
use crate::Error;
use rskafka_proto::{
    apis::{
        fetch::{FetchRequestV4, IsolationLevel, PartitionFetch, TopicFetch},
        metadata::TopicMetadata,
        offset_fetch::TopicOffsets,
    },
    BrokerId, ErrorCode, RecordBatch,
};
use rskafka_wire_format::WireFormatBorrowParse;
use std::collections::HashMap;

pub trait FetchStrategy {
    fn next_fetch(&mut self) -> (BrokerId, FetchRequestV4);
    fn update_fetched(&mut self, r: &FetchResponse);
}

pub struct SimpleFetchStrategy<'a> {
    a: &'a AssignmentContext,
    offsets: Offsets,
    cycle: Box<dyn Iterator<Item = (&'a str, i32)> + Send + 'a>,
    leaders: HashMap<(&'a str, i32), BrokerId>,
}

impl<'a> SimpleFetchStrategy<'a> {
    pub fn new(a: &'a AssignmentContext, offsets: Offsets) -> Self {
        let cycle = a
            .assigned_partitions
            .iter()
            .flat_map(|(t, partitions)| partitions.iter().map(move |p| (t.as_str(), *p)))
            .cycle();

        let leaders = a
            .topic_metadata
            .values()
            .flat_map(|t| {
                t.partitions
                    .iter()
                    .map(move |p| ((t.name.as_str(), p.partition_index), p.leader))
            })
            .collect();

        SimpleFetchStrategy {
            a,
            offsets,
            cycle: Box::new(cycle),
            leaders,
        }
    }
}

impl<'a> FetchStrategy for SimpleFetchStrategy<'a> {
    fn next_fetch(&mut self) -> (BrokerId, FetchRequestV4) {
        let (topic, partition) = self.cycle.next().unwrap();
        let broker = *self.leaders.get(&(topic, partition)).unwrap();
        let request = FetchRequestV4 {
            replica_id: -1,
            max_wait_time: 100,
            min_bytes: 1024,
            max_bytes: 1024 * 1024 * 1024,
            isolation_level: IsolationLevel::ReadCommitted,
            topics: vec![TopicFetch {
                name: topic.to_owned().into(),
                partitions: vec![PartitionFetch {
                    fetch_offset: self.offsets.get(topic, partition).expect("missing offset"), //TODO: error on missing topic
                    index: partition,
                    partition_max_bytes: 1024 * 1024 * 1024,
                }],
            }],
        };

        (broker, request)
    }

    fn update_fetched(&mut self, r: &FetchResponse) {
        r.partitions().for_each(|(t, p)| {
            // let batch = RecordBatch::over_wire_bytes(&p.record_set).unwrap(); // TODO: change batch parsing so it's not parsed twice
            // let offset_val = batch.base_offset
            //     + batch
            //         .records
            //         .as_ref()
            //         .last()
            //         .map(|r| r.offset_delta.0 as i64)
            //         .unwrap_or(0);
            let offset_val = p.high_watermark; //TODO: Hihgly unsure if this is correct value to use as fetched offset
            self.offsets.update(t, p.index, offset_val)
        });
    }
}

#[derive(Debug)]
pub struct Offsets(HashMap<String, HashMap<i32, i64>>);

impl Offsets {
    pub fn from_response(r: Vec<TopicOffsets>) -> Result<Self, Error> {
        let mut topics = HashMap::new();
        for t in r.into_iter() {
            let mut partitions = HashMap::new();
            for p in t.partitions {
                match p.error_code {
                    ErrorCode::None => {
                        partitions.insert(p.index, p.committed_offset + 1);
                    }
                    error => return Err(Error::ErrorResponse(error, "offset fetch error".into())),
                }
            }
            topics.insert(t.name, partitions);
        }

        Ok(Offsets(topics))
    }

    pub fn update(&mut self, topic: &str, partition: i32, offset: i64) {
        let partitions = self.0.get_mut(topic).unwrap(); //TODO: error on missing topic
        *partitions.entry(partition).or_default() = offset;
    }

    pub fn get(&self, topic: &str, partition: i32) -> Option<i64> {
        self.0.get(topic).and_then(|t| t.get(&partition).copied())
    }
}

#[derive(Debug)]
pub struct AssignmentContext {
    pub generation_id: i32,
    pub member_id: String,
    pub assigned_partitions: HashMap<String, Vec<i32>>,
    pub topic_metadata: HashMap<String, TopicMetadata>,
    pub coordinator: BrokerId,
}
