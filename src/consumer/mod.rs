use crate::{
    client::{AsyncClusterClient, Broker},
    message::KafkaPartition,
    Error as RsKafkaError, KafkaMessage, KafkaOffset,
};
use anyhow::{Context as AnyhowContext, Error, Result};
use assignment_stream::{Assignment, AssignmentStream};
use fetch_data::FetchResponse;
use fetch_strategy::{AssignmentContext, FetchStrategy, Offsets, SimpleFetchStrategy};
use futures::prelude::*;
use log::{debug, error, info, log_enabled, trace, warn};
use rskafka_proto::{
    apis::{
        fetch::FetchResponseV4,
        find_coordinator::{self, FindCoordinatorRequestV2, FindCoordinatorResponseV2},
        join_group::{GroupMember, JoinGroupRequestV4, JoinGroupResponseV4, Protocol},
        metadata::{MetadataRequestV2, MetadataResponseV2, TopicMetadata},
        offset_fetch::{
            OffsetFetchRequestV1, OffsetFetchResponseV1, TopicOffsets, TopicPartitions,
        },
        sync_group::{MemberAssignmentData, SyncGroupRequestV2, SyncGroupResponseV2},
    },
    BrokerId, ErrorCode,
};
use rskafka_wire_format::prelude::*;
use std::{
    borrow::Cow,
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    sync::{mpsc, Notify},
    task::JoinHandle,
    time,
};

mod assignment_stream;
mod fetch_data;
mod fetch_strategy;

pub struct ConsumerError(pub Error);

impl ConsumerError {
    pub fn context<C>(self, context: C) -> Self
    where
        C: std::fmt::Display + Send + Sync + 'static,
    {
        ConsumerError(self.0.context(context))
    }
}

impl<E> From<E> for ConsumerError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(e: E) -> Self {
        ConsumerError(anyhow::Error::from(e))
    }
}

pub struct ConsumerConfig {
    pub topics: Vec<String>,
    pub group_id: String,
    pub client_id: String,
}

pub struct Consumer {
    receiver: mpsc::Receiver<Result<Assignment, ConsumerError>>,
    killswitch: ConsumerKillswitch,
}

impl Consumer {
    pub async fn bootstrap<S: AsRef<str>>(
        servers: S,
        config: ConsumerConfig,
    ) -> Result<Self, Error> {
        let cluster = AsyncClusterClient::bootstrap(servers, config.client_id.clone()).await?;
        Ok(Self::with_cluster_client(&Arc::new(cluster), config))
    }

    pub fn with_cluster_client(client: &Arc<AsyncClusterClient>, config: ConsumerConfig) -> Self {
        ConsumerInternals::spawn(Arc::clone(client), config)
    }

    pub fn split(
        self,
    ) -> (
        ConsumerKillswitch,
        impl Stream<Item = Result<Assignment, ConsumerError>>,
    ) {
        (self.killswitch, self.receiver)
    }
}

/// Streaming asynchronous consumer
struct ConsumerInternals {
    cluster: Arc<AsyncClusterClient>,
    config: ConsumerConfig,
}

impl ConsumerInternals {
    // pub async fn bootstrap<S: AsRef<str>>(servers: S, config: ConsumerConfig) -> Result<Self, Error> {
    //     let cluster = AsyncClusterClient::bootstrap(servers, config.client_id).await?;
    //     let consumer = ConsumerInternals {
    //         cluster: Arc::new(cluster),
    //         config
    //     };

    //     Ok(consumer)
    // }

    // pub fn with_cluster_client(client: &Arc<AsyncClusterClient>) -> Self {
    //     Consumer {
    //         cluster: Arc::clone(client),
    //     }
    // }

    // pub async fn consume_from<'a, S: Into<Cow<'a, str>>>(
    //     self,
    //     topics: Vec<S>,
    // ) -> Result<(ConsumerKillswitch, AssignmentStream), Error> {
    //     let topics: Vec<String> = topics.into_iter().map(|s| s.into().into_owned()).collect();
    //     Ok(self.spawn_consumer_task(topics))
    // }

    fn spawn(cluster: Arc<AsyncClusterClient>, config: ConsumerConfig) -> Consumer {
        let (sender, receiver) = mpsc::channel(1);
        let shutdown = Arc::new(Notify::new());

        let internals = ConsumerInternals { cluster, config };

        let join_handle = tokio::spawn({
            let shutdown = Arc::clone(&shutdown);
            internals.consumer_loop(sender, shutdown)
        });

        let killswitch = ConsumerKillswitch {
            shutdown,
            join_handle,
        };

        Consumer {
            receiver,
            killswitch,
        }
    }

    async fn consumer_loop(
        self,
        mut assignment_sender: mpsc::Sender<Result<Assignment, ConsumerError>>,
        shutdown: Arc<Notify>,
    ) {
        let result = self
            .consumer_loop_inner(assignment_sender.clone(), shutdown)
            .await;
        match result {
            Err(e) => {
                let _ = assignment_sender.send(Err(ConsumerError(e.into()))).await;
            }
            _ => {}
        }
    }

    async fn consumer_loop_inner(
        self,
        mut sender: mpsc::Sender<Result<Assignment, ConsumerError>>,
        shutdown: Arc<Notify>,
    ) -> Result<(), Error> {
        // let leaders = self.fetch_partition_leaders().await?;
        let coordinator = self.find_coordinator().await?;

        let mut assignment_context = self.join_group(coordinator, None).await?;
        loop {
            // Send new message stream for assignment
            let (fetch_sender, fetch_receiver) = mpsc::channel(1);
            let (commit_sender, commit_receiver) = mpsc::channel(10);
            let assignment = Assignment::new(fetch_receiver, commit_sender);

            if let Err(_) = sender.send(Ok(assignment)).await {
                debug!("shutting down - Assignment stream receiver deallocated");
                return Ok(());
            }

            match self
                .fetch_loop(&assignment_context, &shutdown, fetch_sender)
                .await
                .context("fetch loop failed")?
            {
                StopKind::RebalanceInProgress => continue,
                StopKind::Shutdown => break,
            }

            assignment_context = self.join_group(coordinator, None).await?;
        }

        //do cleanup stuff
        debug!("Consumer stopped");
        return Ok(());
    }

    async fn fetch_loop(
        &self,
        assignment: &AssignmentContext,
        shutdown: &Notify,
        mut fetch_sender: mpsc::Sender<FetchResponse>,
    ) -> Result<StopKind> {
        let offsets = self.fetch_offsets(&assignment).await?;
        let mut fetch_strategy = SimpleFetchStrategy::new(assignment, offsets);

        loop {
            let (broker, fetch_request) = fetch_strategy.next_fetch();
            trace!(target: "rskafka::fetch", "REQUEST\n{:#?}", fetch_request);
            let fetch_response: FetchResponseV4 = self
                .cluster
                .make_request(fetch_request, Some(broker))
                .await?;
            trace!(target: "rskafka::fetch", "RESPONSE\n{:#?}", fetch_response);
            let fetch = FetchResponse::from(fetch_response);

            //todo: handle errors
            fetch_strategy.update_fetched(&fetch);
            fetch_sender.send(fetch).await.unwrap(); //todo handle error
        }
        shutdown.notified().await;
        Ok(StopKind::Shutdown)
    }

    async fn fetch_offsets(&self, a: &AssignmentContext) -> Result<Offsets, Error> {
        let request = OffsetFetchRequestV1 {
            group_id: Cow::Borrowed(&self.config.group_id),
            topics: a
                .assigned_partitions
                .iter()
                .map(|(t, p)| TopicPartitions {
                    name: t.into(),
                    partition_indexes: p.clone(),
                })
                .collect(),
        };

        let response: OffsetFetchResponseV1 = self
            .cluster
            .make_request(request, Some(a.coordinator))
            .await?;

        response.topics.iter().for_each(|t| {
            t.partitions.iter().for_each(|p| match p.error_code {
                ErrorCode::None => debug!(
                    "Fetched offset {}[{}]: {}",
                    t.name, p.index, p.committed_offset
                ),
                error => error!("Offset fetch error: {}", error),
            })
        });

        Offsets::from_response(response.topics).map_err(Into::into)
    }

    // async fn fetch_partition_leaders(&self) -> Result<HashMap<KafkaPartition, BrokerId>, Error> {
    //     debug!("Fetching metadata for topics: {:?}", self.config.topics);

    //     // Get topics metadata
    //     let response = self.get_topics_metadata(self.ca)
    //     let request = MetadataRequestV2 {
    //         topics: self.config.topics.clone(),
    //     };
    //     let response: MetadataResponseV2 = self.cluster.make_request(request, None).await?;

    //     // Build partition -> leader map
    //     // partition_leaders: HashMap<KafkaPartition, BrokerId>,
    //     let leaders: HashMap<KafkaPartition, BrokerId> = response
    //         .topics
    //         .iter()
    //         .flat_map(|t| {
    //             t.partitions.iter().map(move |p| {
    //                 let leader = p.leader;
    //                 let partition = KafkaPartition {
    //                     topic_name: t.name.clone(),
    //                     partition_index: p.partition_index,
    //                 };
    //                 (partition, leader)
    //             })
    //         })
    //         .collect();

    //     if log_enabled!(log::Level::Trace) {
    //         for (p, leader) in leaders.iter() {
    //             trace!("Found leader for {}: {}", p, leader);
    //         }
    //     }

    //     Ok(leaders)
    // }

    async fn find_coordinator(&self) -> Result<BrokerId> {
        let request = FindCoordinatorRequestV2 {
            key: self.config.group_id.clone(),
            key_type: find_coordinator::KeyType::Group,
        };
        let response: FindCoordinatorResponseV2 = self.cluster.make_request(request, None).await?;

        match response.error_code {
            ErrorCode::None => {
                trace!(
                    "Found coordinator for group {}: {}",
                    self.config.group_id,
                    response.node_id
                );
                Ok(response.node_id)
            }
            error => Err(RsKafkaError::from(error).into()),
        }
    }

    async fn join_group(
        &self,
        coordinator: BrokerId,
        member_id: Option<&str>,
    ) -> Result<AssignmentContext> {
        let mut member_id = member_id.map(Cow::Borrowed);
        let metadata = self.build_group_protocol_metadata();

        let join_group_response: JoinGroupResponseV4 = loop {
            let request = self.build_join_group_request(&metadata, member_id.clone());
            let response: JoinGroupResponseV4 = self
                .cluster
                .make_request(request, Some(coordinator))
                .await?;

            match response.error_code {
                ErrorCode::None => break response,
                ErrorCode::MemberIdRequired if member_id.is_none() => {
                    info!("Assigned member id: {}", response.member_id);
                    member_id = Some(Cow::Owned(response.member_id));
                    continue;
                }
                error => return Err(RsKafkaError::from(error).into()),
            }
        };

        let members_per_topic = Self::extract_members_for_topics(&join_group_response.members);
        let topics_metadata = self
            .get_topics_metadata(members_per_topic.keys().map(String::as_str))
            .await?;

        let sync_group_request = if !join_group_response.members.is_empty() {
            debug!("Performing partition assignment");
            // Assign partitions
            let assignments =
                self.assign_partitions_roundrobin(&members_per_topic, &topics_metadata)?;

            SyncGroupRequestV2 {
                group_id: Cow::Borrowed(&self.config.group_id),
                generation_id: join_group_response.generation_id,
                member_id: Cow::Borrowed(&join_group_response.member_id),
                assignments,
            }
        } else {
            debug!("Requesting partition assignment");
            // Wait for assignment
            SyncGroupRequestV2 {
                group_id: Cow::Borrowed(&self.config.group_id),
                generation_id: join_group_response.generation_id,
                member_id: Cow::Borrowed(&join_group_response.member_id),
                assignments: Vec::new(),
            }
        };

        let sync_group_response: SyncGroupResponseV2 = self
            .cluster
            .make_request(sync_group_request, Some(coordinator))
            .await?;

        match sync_group_response.error_code {
            ErrorCode::None => {
                let assigned =
                    AssignmentMetadata::from_wire_bytes(&sync_group_response.assignment)?.topics;

                if log_enabled!(log::Level::Debug) {
                    for (topic, partitions) in assigned.iter() {
                        for partition in partitions {
                            debug!("Assigned to partition {}[{}]", topic, partition);
                        }
                    }
                }

                Ok(AssignmentContext {
                    generation_id: join_group_response.generation_id,
                    member_id: join_group_response.member_id,
                    assigned_partitions: assigned.into_iter().collect(),
                    topic_metadata: topics_metadata
                        .into_iter()
                        .map(|t| (t.name.clone(), t))
                        .collect(),
                    coordinator,
                })
            }
            error => Err(RsKafkaError::from(error).into()),
        }
    }

    async fn get_topics_metadata<'a, I>(&self, topics: I) -> Result<Vec<TopicMetadata>, Error>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let request = MetadataRequestV2 {
            topics: topics.into_iter().map(|s| s.to_owned()).collect(),
        };
        let response: MetadataResponseV2 = self.cluster.make_request(request, None).await?;

        Ok(response.topics)
    }

    fn build_group_protocol_metadata(&self) -> Vec<u8> {
        let protocol_metadata = GroupProtocolMetadata::new(self.config.topics.as_slice().into());
        protocol_metadata.to_wire_bytes()
    }

    fn build_join_group_request<'a>(
        &'a self,
        protocol_metadata: &'a [u8],
        member_id: Option<Cow<'a, str>>,
    ) -> JoinGroupRequestV4<'a> {
        let member_id = member_id.unwrap_or(Cow::Borrowed(""));
        JoinGroupRequestV4 {
            rebalance_timeout_ms: 10000,
            session_timeout_ms: 30000,
            group_id: self.config.group_id.as_str().into(),
            member_id,
            protocol_type: "consumer".into(),
            protocols: vec![
                Protocol {
                    name: "range".into(),
                    metadata: protocol_metadata.into(),
                },
                Protocol {
                    name: "roundrobin".into(),
                    metadata: protocol_metadata.into(),
                },
            ],
        }
    }

    fn extract_members_for_topics(members: &[GroupMember]) -> HashMap<String, Vec<&str>> {
        let mut members_per_topic: HashMap<String, Vec<&str>> = HashMap::new();
        for m in members {
            let topics: Vec<String> = GroupProtocolMetadataOwned::from_wire_bytes(&m.metadata)
                .unwrap()
                .topics
                .into_iter()
                .map(Into::into)
                .collect();

            for topic in topics.into_iter() {
                members_per_topic
                    .entry(topic)
                    .or_default()
                    .push(&m.member_id)
            }
        }

        members_per_topic
    }

    //todo: unittests!
    fn assign_partitions_roundrobin(
        &self,
        members_per_topic: &HashMap<String, Vec<&str>>,
        topics_metadata: &[TopicMetadata],
    ) -> Result<Vec<MemberAssignmentData<'static>>, Error> {
        let topics_metadata: HashMap<_, _> =
            topics_metadata.into_iter().map(|m| (&m.name, m)).collect();
        // Only roundrobin supported as for now

        let mut assignments_per_member: HashMap<&str, HashMap<&str, Vec<i32>>> = HashMap::new();
        for (topic, members) in members_per_topic {
            let t_meta = topics_metadata.get(&topic).unwrap(); // Metadata existence should be guaranteed earlier
            t_meta
                .partitions
                .iter()
                .map(|p| p.partition_index)
                .zip(members.iter().cycle())
                .for_each(|(idx, member_id)| {
                    assignments_per_member
                        .entry(*member_id)
                        .or_default()
                        .entry(&t_meta.name)
                        .or_default()
                        .push(idx)
                });
        }

        let assignments: Vec<(String, Vec<(String, Vec<i32>)>)> = assignments_per_member
            .into_iter()
            .map(|(member_id, topics)| {
                (
                    member_id.to_string(),
                    topics
                        .into_iter()
                        .map(|(topic, partitions)| (topic.to_owned(), partitions))
                        .collect(),
                )
            })
            .collect();

        if log_enabled!(log::Level::Debug) {
            Self::log_assingment(&assignments);
        }

        let assignments = assignments
            .into_iter()
            .map(|(member_id, topics)| MemberAssignmentData {
                member_id: Cow::Owned(member_id),
                assignment: AssignmentMetadata::new(topics).to_wire_bytes(),
            })
            .collect();

        Ok(assignments)
    }

    fn log_assingment(assignments: &[(String, Vec<(String, Vec<i32>)>)]) {
        for (member, topics) in assignments.iter() {
            for (topic, partitions) in topics {
                for partition in partitions {
                    debug!("Assigning {}[{}] to {}", topic, partition, member)
                }
            }
        }
    }
}

enum StopKind {
    Shutdown,
    RebalanceInProgress,
}

//todo: rename to sth like: Private/Custom
//todo: get rid of cow and as ref for simplicity
#[derive(Debug, Clone, WireFormatWrite, WireFormatParse)]
struct GroupProtocolMetadata<'a, S: AsRef<str> + Clone> {
    _placeholder_head: i16,
    pub topics: Cow<'a, [S]>,
    _placeholder_tail: i32,
}

type GroupProtocolMetadataOwned = GroupProtocolMetadata<'static, String>;

impl<'a, S: AsRef<str> + Clone> GroupProtocolMetadata<'a, S> {
    pub fn new(topics: Cow<'a, [S]>) -> Self {
        GroupProtocolMetadata {
            topics,
            _placeholder_head: 0,
            _placeholder_tail: 0,
        }
    }
}

//todo: rename to sth like: Private/Custom
#[derive(Debug, Clone, WireFormatWrite, WireFormatParse)]
struct AssignmentMetadata {
    _placeholder_head: i16,
    pub topics: Vec<(String, Vec<i32>)>,
    _placeholder_tail: i32,
}

impl AssignmentMetadata {
    pub fn new(topics: Vec<(String, Vec<i32>)>) -> Self {
        AssignmentMetadata {
            topics,
            _placeholder_head: 0,
            _placeholder_tail: 0,
        }
    }
}

pub struct ConsumerKillswitch {
    shutdown: Arc<Notify>,
    join_handle: JoinHandle<()>,
}

impl ConsumerKillswitch {
    pub async fn shutdown(self) {
        info!("Shutting down consumer");
        self.shutdown.notify();
        let _ = self.join_handle.await;
    }
}
