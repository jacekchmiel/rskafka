use super::Error;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub(crate) bootstrap_servers: String,
    pub(crate) request_timeout: Duration,
    pub(crate) tcp_write_timeout: Duration,
    pub(crate) tcp_read_timeout: Duration,
    pub(crate) client_id: String,
}

impl ClientConfig {
    pub fn builder() -> ClientConfigBuilder {
        ClientConfigBuilder::default()
    }
}

pub struct ClientConfigBuilder {
    bootstrap_servers: Option<String>,
    request_timeout: Duration,
    tcp_write_timeout: Duration,
    tcp_read_timeout: Duration,
}

impl Default for ClientConfigBuilder {
    fn default() -> Self {
        ClientConfigBuilder {
            bootstrap_servers: None,
            request_timeout: Duration::from_secs(10),
            tcp_read_timeout: Duration::from_secs(1),
            tcp_write_timeout: Duration::from_secs(1),
        }
    }
}

impl ClientConfigBuilder {
    pub fn bootstrap_servers(mut self, val: String) -> Self {
        self.bootstrap_servers = Some(val);
        self
    }

    pub fn build(self) -> Result<ClientConfig, Error> {
        let bootstrap_servers = self
            .bootstrap_servers
            .map(Ok)
            .unwrap_or(Err(Error::IncompleteConfig("bootstrap_servers")))?;

        Ok(ClientConfig {
            bootstrap_servers,
            request_timeout: self.request_timeout,
            tcp_read_timeout: self.tcp_read_timeout,
            tcp_write_timeout: self.tcp_write_timeout,
            client_id: "rskafka".to_string(),
        })
    }
}
