use super::async_connection::{BrokerConnection, Managed};
use crate::{
    proto::{apis::metadata::MetadataRequestV2, data::BrokerId, KafkaRequest},
    Error,
};
use log::{debug, error, info, log_enabled};
use std::collections::HashMap;
use tokio::sync::{Mutex, MutexGuard};

pub struct AsyncClusterClient {
    conns: HashMap<BrokerId, Mutex<Managed>>,
}

impl AsyncClusterClient {
    pub async fn bootstrap<S: AsRef<str>>(
        bootstrap_servers: S,
        client_id: String,
    ) -> Result<Self, Error> {
        let bootstrap_servers: Vec<_> = bootstrap_servers.as_ref().split(',').collect();
        info!("Bootstraping client using: {:?}", bootstrap_servers);
        for server in bootstrap_servers {
            match Self::try_bootstrap_from(server, client_id.clone()).await {
                Ok(client) => return Ok(client),
                Err(e) => error!("Bootstrap failed: {}", e),
            }
        }

        Err(Error::ClusterError(
            "failed to contact any bootstrap server".into(),
        ))
    }

    async fn try_bootstrap_from(server_addr: &str, client_id: String) -> Result<Self, Error> {
        let mut conn = BrokerConnection::connect(server_addr, "rskafka-async".to_string()).await?;
        let request = MetadataRequestV2 { topics: Vec::new() };
        let response = conn.make_request(&request).await?;
        let addrs: HashMap<BrokerId, String> = response
            .brokers
            .into_iter()
            .map(|b| (b.node_id, format!("{}:{}", b.host, b.port)))
            .collect();

        if log_enabled!(log::Level::Debug) {
            let mut nodes: Vec<_> = addrs.iter().collect();
            nodes.sort_by_key(|n| n.1);
            for n in nodes {
                debug!("Discovered broker {} - {}", n.0, n.1)
            }
        }

        let conns = addrs
            .into_iter()
            .map(|(broker, addr)| (broker, Mutex::new(Managed::new(addr, client_id.clone()))))
            .collect();

        Ok(AsyncClusterClient { conns })
    }

    pub(crate) async fn make_request<R: KafkaRequest>(
        &self,
        r: R,
        broker: Broker,
    ) -> Result<R::Response, Error> {
        let mut managed = self.get_connection(broker).await;
        let conn = managed.get().await?;
        conn.make_request(&r).await
    }

    async fn get_connection<'a>(&'a self, broker: Broker) -> MutexGuard<'a, Managed> {
        match broker {
            Broker::Any => self.conns.values().next().unwrap().lock().await,
            Broker::Id(id) => todo!(),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Broker {
    Any,
    Id(BrokerId),
}

// struct ConnectionManager {
//     addrs: HashMap<BrokerId, String>,
//     conns: HashMap<BrokerId, Mutex<AsyncBrokerConnection>>,
// }

// impl ConnectionManager {
//     async fn get_connection<'a>(&'a self, broker: Broker) -> MutexGuard<'a, AsyncBrokerConnection> {
// }

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::init_logger;

    #[tokio::test]
    async fn can_bootstrap() -> Result<(), Error> {
        init_logger();
        let _cluster_client =
            AsyncClusterClient::bootstrap("localhost:9092", "rskafka-test".into()).await?;

        Ok(())
    }
}
