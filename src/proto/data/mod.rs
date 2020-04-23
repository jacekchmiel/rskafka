pub mod api_key;
pub mod error;
pub mod header;
pub mod primitive;

use super::{KafkaRequest, KafkaWireFormatWrite};
use byteorder::{BigEndian, WriteBytesExt};
use header::RequestHeader;
use log::{log_enabled, trace};
use std::borrow::Cow;

pub(crate) fn write_request<W: std::io::Write, R: KafkaRequest>(
    mut writer: W,
    request: &R,
    correlation_id: i32,
    client_id: Option<&str>,
) -> std::io::Result<usize> {
    let header = RequestHeader {
        request_api_key: R::API_KEY,
        request_api_version: R::API_VERSION,
        correlation_id,
        client_id: client_id.map(Cow::Borrowed),
    };
    let size = header.serialized_size() + request.serialized_size();

    trace!("write_request: size={:?}", size);
    trace!("write_request: header={:?}", header);

    if log_enabled!(log::Level::Trace) {
        let mut buffer = Vec::with_capacity(size as usize + std::mem::size_of_val(&size));
        buffer.write_i32::<BigEndian>(size as i32)?; //TODO: conversion error
        header.write_into(&mut buffer)?;
        request.write_into(&mut buffer)?;
        trace!("write_request: {:?}", buffer);

        writer.write_all(&buffer)?;
    } else {
        writer.write_i32::<BigEndian>(size as i32)?; //TODO: conversion error
        header.write_into(&mut writer)?;
        request.write_into(&mut writer)?;
    }

    writer.flush()?;

    Ok(0)
}

#[cfg(test)]
pub(crate) fn request_bytes<R: KafkaRequest>(
    request: &R,
    correlation_id: i32,
    client_id: Option<&str>,
) -> Vec<u8> {
    let mut buffer = Vec::new();
    write_request(&mut buffer, request, correlation_id, client_id).expect("write to vec failed");

    buffer
}
