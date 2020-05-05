use crate::{data::header::RequestHeader, ApiKey, KafkaResponse};
use log::{log_enabled, trace};
use rskafka_wire_format::prelude::*;

/// Represents a concrete request in Kafka Protocol wire format.
/// Has specific Api Key, Api Version and can be written in wire format.
pub trait KafkaRequest: WireFormatWrite {
    const API_KEY: ApiKey;
    const API_VERSION: i16;
    type Response: KafkaResponse;

    fn write_bytes<W: std::io::Write>(
        &self,
        mut writer: W,
        correlation_id: i32,
        client_id: Option<&str>,
    ) -> std::io::Result<usize> {
        let header = RequestHeader {
            request_api_key: Self::API_KEY,
            request_api_version: Self::API_VERSION,
            correlation_id,
            client_id: client_id.into(),
        };
        let size = header.wire_size() + self.wire_size();

        trace!("write: size={:?}", size);
        trace!("write: header={:?}", header);

        if log_enabled!(log::Level::Trace) {
            let mut buffer = Vec::with_capacity(size as usize + std::mem::size_of_val(&size));
            (size as i32).write_into(&mut buffer)?; //TODO: conversion error
            header.write_into(&mut buffer)?;
            self.write_into(&mut buffer)?;
            trace!("write: {:?}", buffer);

            writer.write_all(&buffer)?;
        } else {
            (size as i32).write_into(&mut writer)?; //TODO: conversion error
            header.write_into(&mut writer)?;
            self.write_into(&mut writer)?;
        }

        writer.flush()?;

        Ok(0)
    }

    fn to_bytes(&self, correlation_id: i32, client_id: Option<&str>) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.write_bytes(&mut buffer, correlation_id, client_id)
            .expect("write to vec failed");

        buffer
    }
}
