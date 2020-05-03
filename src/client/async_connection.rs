use crate::Error;
use log::{debug, trace};
use rskafka_proto::{
    apis::api_versions::{ApiVersionsRange, ApiVersionsRequestV0},
    ErrorCode, KafkaRequest, KafkaResponse,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct BrokerConnection {
    stream: TcpStream,
    last_correlation_id: i32,
    client_id: String,
}

impl BrokerConnection {
    pub(crate) async fn connect<A: tokio::net::ToSocketAddrs>(
        addr: A,
        client_id: String,
    ) -> Result<Self, Error> {
        let stream = TcpStream::connect(addr).await?;
        Ok(BrokerConnection {
            stream,
            last_correlation_id: 0,
            client_id,
        })
    }

    pub(crate) async fn get_api_versions(&mut self) -> Result<Vec<ApiVersionsRange>, Error> {
        debug!("get_api_versions");
        let response = self.make_request(&ApiVersionsRequestV0).await?;
        match response.error_code {
            ErrorCode(0) => Ok(response.api_keys),
            error_code => Err(error_code.into()),
        }
    }

    pub(crate) async fn make_request<'a, Req: KafkaRequest>(
        &mut self,
        request: &Req,
    ) -> Result<Req::Response, Error> {
        // self.ensure_api_supported(request)?;
        self.last_correlation_id += 1;
        let mut request_buffer = Vec::new();

        request.write_bytes(
            &mut request_buffer,
            self.last_correlation_id,
            Some(&self.client_id),
        )?;

        self.stream.write_all(&request_buffer).await?;
        let response_bytes = self.read_response(self.last_correlation_id).await?;
        let response = Req::Response::from_bytes(&response_bytes)?;

        Ok(response)
    }

    async fn read_response(&mut self, correlation_id: i32) -> Result<Vec<u8>, Error> {
        let size = self.read_be_i32().await?; //TODO: check if size > 4
        trace!("read_response: size={}", size);
        let recv_correlation_id = self.read_be_i32().await?;
        trace!("read_response: correlation_id={}", recv_correlation_id);

        let mut data_buffer = vec![0; (size - 4) as usize]; //TODO: conversion error
        if let Err(e) = self.stream.read_exact(data_buffer.as_mut()).await {
            return Err(e.into());
        }

        trace!("read_response: response_bytes={:?}", data_buffer);
        if recv_correlation_id == correlation_id {
            Ok(data_buffer)
        } else {
            return Err(Error::ProtocolError(
                format!(
                    "unexpected correlation_id={}, expected correlation_id={}",
                    recv_correlation_id, correlation_id
                )
                .into(),
            ));
        }
    }

    async fn read_be_i32(&mut self) -> Result<i32, Error> {
        let mut buffer: [u8; 4] = [0, 0, 0, 0];
        self.stream.read_exact(&mut buffer).await?;
        Ok(i32::from_be_bytes(buffer))
    }
}

pub struct Managed {
    addr: String,
    client_id: String,
    conn: Option<BrokerConnection>,
}

impl Managed {
    pub(crate) fn new(addr: String, client_id: String) -> Self {
        Managed {
            addr,
            client_id,
            conn: None,
        }
    }

    pub async fn get(&mut self) -> Result<&mut BrokerConnection, Error> {
        //TODO try remove unwrap
        if self.conn.is_none() {
            self.conn =
                Some(BrokerConnection::connect(self.addr.clone(), self.client_id.clone()).await?);
        }

        Ok(self.conn.as_mut().unwrap())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use log::info;
    use rskafka_proto::apis::{
        create_topics::{CreateTopic, CreateTopicsRequestV1},
        metadata::MetadataRequestV2,
    };

    fn init_logger() {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init()
            .ok();
    }

    async fn connect() -> Result<BrokerConnection, Error> {
        BrokerConnection::connect("localhost:9092", String::from("rskafka-async")).await
    }

    #[tokio::test]
    async fn connect_to_broker() -> Result<(), Error> {
        let _c = connect().await?;

        Ok(())
    }

    #[tokio::test]
    async fn read_versions() -> Result<(), Error> {
        init_logger();
        let mut c = connect().await?;

        let versions = c.get_api_versions().await?;
        info!("Supported versions:\n{:#?}", versions);

        Ok(())
    }

    #[tokio::test]
    async fn read_metadata() -> Result<(), Error> {
        init_logger();
        let mut c = connect().await?;
        let request = MetadataRequestV2 {
            topics: vec![String::from("test-topic")],
        };

        let metadata = c.make_request(&request).await?;
        info!("Metadata:\n{:#?}", metadata);

        Ok(())
    }

    #[tokio::test]
    async fn create_topic() -> Result<(), Error> {
        init_logger();
        let mut c = connect().await?;

        let request = CreateTopicsRequestV1 {
            topics: vec![CreateTopic::with_name("rskafka-create-topic-test")
                .partitions(2)
                .replication_factor(1)],
            timeout_ms: 1000,
            validate_only: false,
        };
        let response = c.make_request(&request).await?;

        info!("Response:\n{:#?}", response);

        Ok(())
    }
}
