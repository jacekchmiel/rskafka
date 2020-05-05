use crate::{
    client::{AsyncClusterClient, Broker},
    message::KafkaPartition,
    Error, KafkaMessage, KafkaOffset,
};
use futures::prelude::*;
use futures::ready;
use log::{debug, error, info, log_enabled, trace, warn};
use rskafka_proto::{
    apis::{
        find_coordinator::{self, FindCoordinatorRequestV2, FindCoordinatorResponseV2},
        join_group::{GroupMember, JoinGroupRequestV4, JoinGroupResponseV4, Protocol},
        metadata::{MetadataRequestV2, MetadataResponseV2, TopicMetadata},
        sync_group::{Assignment, SyncGroupRequestV2, SyncGroupResponseV2},
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

pub struct ConsumerConfig {
    pub topics: Vec<String>,
    pub group_id: String,
    pub client_id: String,
}

pub struct Consumer {
    receiver: mpsc::Receiver<Result<MessageStream, Error>>,
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
        impl Stream<Item = Result<MessageStream, Error>>,
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
        mut sender: mpsc::Sender<Result<MessageStream, Error>>,
        shutdown: Arc<Notify>,
    ) {
        let result = self.consumer_loop_inner(sender.clone(), shutdown).await;
        match result {
            Err(e) => {
                let _ = sender.send(Err(e)).await;
            }
            _ => {}
        }
    }

    async fn consumer_loop_inner(
        self,
        mut sender: mpsc::Sender<Result<MessageStream, Error>>,
        shutdown: Arc<Notify>,
    ) -> Result<(), Error> {
        let leaders = self.fetch_partition_leaders().await?;
        let coordinator = self.find_coordinator().await?;

        let mut assignment = self.join_group(coordinator, None).await?;
        loop {
            // Send new message stream for assignment
            let (msg_sender, msg_receiver) = mpsc::channel(10);
            let (commit_sender, commit_receiver) = mpsc::channel(10);
            let message_stream = MessageStream::new(msg_receiver, commit_sender);

            if let Err(_) = sender.send(Ok(message_stream)).await {
                debug!("shutting down - Assignment stream receiver deallocated");
                return Ok(());
            }

            match self.fetch_loop(&assignment, &shutdown).await? {
                StopKind::RebalanceInProgress => continue,
                StopKind::Shutdown => break,
            }

            assignment = self.join_group(coordinator, None).await?;
        }

        //do cleanup stuff
        debug!("Consumer stopped");
        return Ok(());
    }

    async fn fetch_loop(
        &self,
        assignment: &AssignmentData,
        shutdown: &Notify,
    ) -> Result<StopKind, Error> {
        info!("Fetching not implemented yet. I'm doing nothing!");
        shutdown.notified().await;
        Ok(StopKind::Shutdown)
    }

    async fn fetch_partition_leaders(&self) -> Result<HashMap<KafkaPartition, BrokerId>, Error> {
        debug!("Fetching metadata for topics: {:?}", self.config.topics);

        // Get topics metadata
        let request = MetadataRequestV2 {
            topics: self.config.topics.clone(),
        };
        let response: MetadataResponseV2 = self.cluster.make_request(request, None).await?;

        // Build partition -> leader map
        // partition_leaders: HashMap<KafkaPartition, BrokerId>,
        let leaders: HashMap<KafkaPartition, BrokerId> = response
            .topics
            .iter()
            .flat_map(|t| {
                t.partitions.iter().map(move |p| {
                    let leader = p.leader;
                    let partition = KafkaPartition {
                        topic_name: t.name.clone(),
                        partition_index: p.partition_index,
                    };
                    (partition, leader)
                })
            })
            .collect();

        if log_enabled!(log::Level::Trace) {
            for (p, leader) in leaders.iter() {
                trace!("Found leader for {}: {}", p, leader);
            }
        }

        Ok(leaders)
    }

    async fn find_coordinator(&self) -> Result<BrokerId, Error> {
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
            error => Err(error.into()),
        }
    }

    async fn join_group(
        &self,
        coordinator: BrokerId,
        member_id: Option<&str>,
    ) -> Result<AssignmentData, Error> {
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
                error => return Err(error.into()),
            }
        };

        let sync_group_request = if !join_group_response.members.is_empty() {
            debug!("Performing partition assignment");
            // Assign partitions
            let members_per_topic = Self::extract_members_for_topics(&join_group_response.members);

            // request topic metadata again (we may not have complete topic set for group)
            let topics_metadata = self
                .get_topics_metadata(members_per_topic.keys().map(String::as_str))
                .await?;

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

                Ok(AssignmentData {
                    generation_id: join_group_response.generation_id,
                    member_id: join_group_response.member_id,
                    assigned_partitions: assigned,
                })
            }
            error => Err(error.into()),
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
    ) -> Result<Vec<Assignment<'static>>, Error> {
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
            for (member, topics) in assignments.iter() {
                for (topic, partitions) in topics {
                    for partition in partitions {
                        debug!("Assigning {}[{}] to {}", topic, partition, member)
                    }
                }
            }
        }

        let assignments = assignments
            .into_iter()
            .map(|(member_id, topics)| Assignment {
                member_id: Cow::Owned(member_id),
                assignment: AssignmentMetadata::new(topics).to_wire_bytes(),
            })
            .collect();

        Ok(assignments)
    }
}

enum StopKind {
    Shutdown,
    RebalanceInProgress,
}

struct AssignmentData {
    pub generation_id: i32,
    pub member_id: String,
    pub assigned_partitions: Vec<(String, Vec<i32>)>,
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

// impl Stream for Consumer {
//     type Item = Result<MessageStream, Error>;

//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
//         self.receiver.poll_next_unpin(cx)
//     }
// }

pub struct MessageStream {
    msg_receiver: mpsc::Receiver<()>,
    commit_sender: mpsc::Sender<()>,
}

impl MessageStream {
    fn new(msg_receiver: mpsc::Receiver<()>, commit_sender: mpsc::Sender<()>) -> Self {
        MessageStream {
            msg_receiver,
            commit_sender,
        }
    }

    pub fn commit_sink(&self) -> CommitSink {
        CommitSink
    }
}

impl Stream for MessageStream {
    type Item = KafkaMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        warn!("MessageStream::poll_next not implemented");

        let item = ready!(self.msg_receiver.poll_next_unpin(cx));

        Poll::Ready(item.map(|_| todo!()))
    }
}

pub struct CommitSink;

impl Sink<KafkaOffset> for CommitSink {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        warn!("CommitSink::poll_ready not implemented");
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: KafkaOffset) -> Result<(), Self::Error> {
        warn!("CommitSink::start_send not implemented");
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        warn!("MessageStream::poll_flush not implemented");
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        warn!("MessageStream::poll_close not implemented");
        Poll::Ready(Ok(()))
    }
}
