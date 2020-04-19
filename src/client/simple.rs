use super::{ClientConfig, Error};
use crate::proto;
use log::trace;
use proto::{
    apis::api_versions::{ApiVersionsRange, ApiVersionsV0Request, ApiVersionsV0Response},
    data::write_request,
    KafkaWireFormatParse,
};
use std::io::Read;
use std::{
    net::TcpStream,
    time::{Duration, Instant},
};

pub struct SimpleClient {
    config: ClientConfig,
    stream: TcpStream,
}

impl SimpleClient {
    pub fn connect(config: ClientConfig) -> Result<Self, Error> {
        let stream = TcpStream::connect(&config.bootstrap_servers)?;
        stream.set_read_timeout(Some(config.tcp_read_timeout))?;
        stream.set_write_timeout(Some(config.tcp_write_timeout))?;
        Ok(SimpleClient { config, stream })
    }

    pub fn get_api_versions(&mut self) -> Result<Vec<ApiVersionsRange>, Error> {
        let correlation_id = 1;
        write_request(
            &self.stream,
            &ApiVersionsV0Request,
            correlation_id,
            Some(&self.config.client_id),
        )?;

        let response: ApiVersionsV0Response = self.wait_for_response(correlation_id)?;
        match response.error {
            Some(error_code) => Err(Error::ErrorResponse(error_code)),
            None => Ok(response.api_keys),
        }
    }

    fn wait_for_response<R: KafkaWireFormatParse>(
        &mut self,
        correlation_id: i32,
    ) -> Result<R, Error> {
        trace!("wait_for_response: correlation_id={}", correlation_id);
        let timer = Timer::new(self.config.request_timeout);

        let mut size_buffer: [u8; 4] = [0, 0, 0, 0];
        self.stream.set_read_timeout(Some(timer.left()))?;
        if let Err(e) = self.stream.read_exact(&mut size_buffer) {
            //TODO: continue on timeout
            return Err(e.into());
        }

        let size = i32::from_be_bytes(size_buffer);

        trace!("wait_for_response: received_size_bytes={:?}", size_buffer);
        trace!("wait_for_response: received_size={}", size);

        let mut data_buffer = vec![0; size as usize]; //TODO: conversion error

        self.stream.set_read_timeout(Some(timer.left()))?;
        if let Err(e) = self.stream.read_exact(data_buffer.as_mut()) {
            //TODO: continue on timeout
            return Err(e.into());
        }

        trace!("wait_for_response: received_bytes={:?}", data_buffer);
        let (response_bytes, header) = proto::parse_header(&data_buffer)?;
        if header.correlation_id == correlation_id {
            return Ok(R::from_wire_bytes(&response_bytes)?);
        } else {
            return Err(Error::ProtocolError(format!(
                "unexpected correlation_id={}, expected correlation_id={}",
                header.correlation_id, correlation_id
            )));
        }
    }
}

struct Timer {
    start: Instant,
    timeout: Duration,
}

impl Timer {
    pub fn new(timeout: Duration) -> Self {
        Timer {
            start: Instant::now(),
            timeout,
        }
    }

    pub fn left(&self) -> Duration {
        self.timeout
            .checked_sub(self.start.elapsed())
            .unwrap_or(Duration::from_secs(0))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_client() {
        let _ = SimpleClient::connect(
            ClientConfig::builder()
                .bootstrap_servers("localhost:9092".to_string())
                .build()
                .expect("ClientConfig build failed"),
        );
    }
}
