use crate::{
    client::{AsyncClusterClient, Broker},
    message::KafkaPartition,
    Error, KafkaMessage, KafkaOffset,
};
use futures::prelude::*;
use log::{debug, error, info, log_enabled, trace};
use rskafka_proto::{
    apis::{
        find_coordinator::{self, FindCoordinatorRequestV2, FindCoordinatorResponseV2},
        join_group::{JoinGroupRequestV4, JoinGroupResponseV4},
        metadata::{MetadataRequestV2, MetadataResponseV2},
    },
    BrokerId, ErrorCode,
};
use std::{
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
        let leaders = self.fetch_partition_leaders().await?; //TODO: send error to AssignmentStream
        let coordinator = self.find_coordinator().await?;
        loop {
            let sender = sender;
            let assignment = self.join_group(coordinator).await?;

            //JoinGroup
            error!("JoinGroup not implemented!");
            shutdown.notified().await;
            info!("Graceful shutdown requested");
            return Ok(());

            // if let Err(_) = sender.send(MessageStream).await {
            //     error!("Graceful cleanup not implemented!");
            //     return;
            // }

            // loop {
            //     // send HeartBeat
            //     time::delay_for(Duration::from_secs(1)).await;
            // }
        }
    }

    async fn fetch_partition_leaders(&self) -> Result<HashMap<KafkaPartition, BrokerId>, Error> {
        debug!("Fetching metadata for topics: {:?}", self.config.topics);

        // Get topics metadata
        let request = MetadataRequestV2 {
            topics: self.config.topics.clone(),
        };
        let response: MetadataResponseV2 = self.cluster.make_request(request, Broker::Any).await?;

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
        let response: FindCoordinatorResponseV2 =
            self.cluster.make_request(request, Broker::Any).await?;

        match response.error_code {
            ErrorCode(0) => {
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

    async fn join_group(&self, coordinator: BrokerId) -> Result<(), Error> {
        let request = JoinGroupRequestV4 {
            rebalance_timeout_ms: 10000,
            session_timeout_ms: 30000,
            group_id: self.config.group_id.as_str().into(),
            member_id: "".into(),
            protocol_type: "consumer".into(),
            protocols: vec![],
        };
        let response: JoinGroupResponseV4 = self
            .cluster
            .make_request(request, Broker::Id(coordinator))
            .await?;

        Ok(())
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

pub struct MessageStream;

impl MessageStream {
    pub fn commit_sink(&self) -> CommitSink {
        todo!()
    }
}

impl Stream for MessageStream {
    type Item = KafkaMessage;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub struct CommitSink;

impl Sink<KafkaOffset> for CommitSink {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn start_send(self: Pin<&mut Self>, item: KafkaOffset) -> Result<(), Self::Error> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        todo!()
    }
}

// #[derive(Debug, Error)]
// #[error("consumer error")]
// pub struct ConsumerError;
