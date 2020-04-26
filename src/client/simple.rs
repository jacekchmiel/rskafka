use super::{ClientConfig, Error};
use crate::proto;
use log::{debug, trace};
use proto::{
    apis::{
        api_versions::{ApiVersionsRange, ApiVersionsV0Request, ApiVersionsV0Response},
        topic_metadata::{MetadataRequestV2, MetadataResponseV2, TopicMetadata},
    },
    data::{api_key::ApiKey, header::ResponseHeader, write_request},
    KafkaRequest, KafkaWireFormatParse,
};
use std::io::Read;
use std::{
    collections::HashMap,
    net::TcpStream,
    time::{Duration, Instant},
};

pub struct SimpleClient {
    config: ClientConfig,
    stream: TcpStream,
    api_versions: Option<HashMap<ApiKey, ApiVersionsRange>>,
    last_correlation_id: i32,
}

impl SimpleClient {
    pub fn connect(config: ClientConfig) -> Result<Self, Error> {
        let stream = TcpStream::connect(&config.bootstrap_servers)?;
        stream.set_read_timeout(Some(config.tcp_read_timeout))?;
        stream.set_write_timeout(Some(config.tcp_write_timeout))?;
        Ok(SimpleClient {
            config,
            stream,
            api_versions: None,
            last_correlation_id: 0,
        })
    }

    pub fn get_api_versions(&mut self) -> Result<Vec<ApiVersionsRange>, Error> {
        debug!("get_api_versions");
        let response: ApiVersionsV0Response = self.exchange(&ApiVersionsV0Request)?;
        match response.error {
            Some(error_code) => Err(Error::ErrorResponse(error_code)),
            None => Ok(response.api_keys),
        }
    }

    pub fn get_metadata(&mut self, topics: Vec<String>) -> Result<MetadataResponseV2, Error> {
        debug!("get_metadata topics={:?}", topics);
        let request = MetadataRequestV2 { topics };
        self.ensure_api_supported(&request)?;
        let response: MetadataResponseV2 = self.exchange(&request)?;

        Ok(response)
    }

    fn exchange<'a, Req: KafkaRequest, Rsp: KafkaWireFormatParse>(
        &mut self,
        request: &Req,
    ) -> Result<Rsp, Error> {
        self.last_correlation_id += 1;
        write_request(
            &self.stream,
            request,
            self.last_correlation_id,
            Some(&self.config.client_id),
        )?;

        let response_bytes = self.wait_for_response(self.last_correlation_id)?;
        let response = Rsp::from_wire_bytes(response_bytes.data())?;

        Ok(response)
    }

    fn api_versions(&mut self) -> Result<&HashMap<ApiKey, ApiVersionsRange>, Error> {
        if self.api_versions.is_none() {
            let versions = self.get_api_versions()?;
            self.api_versions = Some(versions.into_iter().map(|v| (v.api_key, v)).collect());
            debug!("Supported api versions");
            for v in self.api_versions.as_ref().unwrap().values() {
                debug!("{}", v)
            }
        }
        Ok(&self.api_versions.as_ref().unwrap())
    }

    fn is_api_supported(&mut self, key: &ApiKey, version: i16) -> Result<bool, Error> {
        let versions = self.api_versions()?;
        let supported = versions
            .get(key)
            .map(|range| range.min_version <= version && version <= range.max_version)
            .unwrap_or(false);

        Ok(supported)
    }

    fn ensure_api_supported<R: KafkaRequest>(&mut self, r: &R) -> Result<(), Error> {
        if self.is_api_supported(&r.api_key(), r.api_version())? {
            Ok(())
        } else {
            Err(Error::ApiNotSupported(r.api_key(), r.api_version()))
        }
    }

    fn wait_for_response(&mut self, correlation_id: i32) -> Result<ResponseBuffer, Error> {
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
        let header = ResponseHeader::from_wire_bytes_buffer(&data_buffer)?;
        if header.correlation_id == correlation_id {
            Ok(ResponseBuffer(data_buffer))
        // return Ok(R::from_wire_bytes(&response_bytes)?);
        } else {
            return Err(Error::ProtocolError(format!(
                "unexpected correlation_id={}, expected correlation_id={}",
                header.correlation_id, correlation_id
            )));
        }
    }
}

pub struct ResponseBuffer(Vec<u8>);

impl ResponseBuffer {
    const HEADER_SIZE: usize = 4;

    pub fn correlation_id(&self) -> i32 {
        //this struct is created only after successfully parsing header in the first place so unwrap here is okay-ish
        let header = ResponseHeader::from_wire_bytes(&self.0).unwrap();

        header.correlation_id
    }

    pub fn data(&self) -> &[u8] {
        &self.0[Self::HEADER_SIZE..]
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
