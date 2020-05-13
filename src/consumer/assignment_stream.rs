use super::fetch_data::FetchResponse;
use crate::{Error, KafkaMessage, KafkaOffset};
use futures::{prelude::*, stream};
use log::warn;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc;

/// StreamingConsumer
pub struct AssignmentStream {
    receiver: mpsc::Receiver<Assignment>,
}

impl AssignmentStream {
    pub(super) fn new(receiver: mpsc::Receiver<Assignment>) -> Self {
        AssignmentStream { receiver }
    }

    pub fn into_fetch_stream(self) -> impl Stream<Item = FetchResponse> {
        self.map(|assignment| assignment.into_fetch_stream())
            .flatten()
    }

    pub fn into_message_stream(self) -> impl Stream<Item = KafkaMessage<'static>> {
        self.into_fetch_stream()
            .map(|f| stream::iter(f.into_messages_owned()))
            .flatten()
    }
}

impl Stream for AssignmentStream {
    type Item = Assignment;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.receiver.poll_next_unpin(cx)
    }
}

/// StreamingAssignment
#[derive(Debug)]
pub struct Assignment {
    fetch_receiver: mpsc::Receiver<FetchResponse>,
    commit_sender: mpsc::Sender<KafkaOffset<'static>>,
}

impl Assignment {
    pub(super) fn new(
        fetch_receiver: mpsc::Receiver<FetchResponse>,
        commit_sender: mpsc::Sender<KafkaOffset<'static>>,
    ) -> Self {
        Assignment {
            fetch_receiver,
            commit_sender,
        }
    }

    pub fn into_fetch_stream(self) -> impl Stream<Item = FetchResponse> {
        self.fetch_receiver
    }

    pub fn into_message_stream(self) -> impl Stream<Item = KafkaMessage<'static>> {
        self.fetch_receiver
            .map(|f| stream::iter(f.into_messages_owned()))
            .flatten()
    }

    pub fn commit_sink(&self) -> CommitSink {
        CommitSink
    }
}

pub struct CommitSink;

impl Sink<KafkaOffset<'static>> for CommitSink {
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
