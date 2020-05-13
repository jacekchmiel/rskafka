// use futures::{prelude::*, stream};
// use std::borrow::Cow;
// use tokio::sync::mpsc;

// struct AssignmentStream {
//     receiver: mpsc::Receiver<FetchResponseStream>,
// }

// impl AssignmentStream {
//     pub fn into_fetch_stream(self) -> impl Stream<Item = FetchResponse> {
//         self.map(|fstream| fstream).flatten()
//     }

//     pub fn into_message_stream(self) -> impl Stream<Item = KafkaMessage<'static>> {
//         self.into_fetch_stream()
//             .map(|f| stream::iter(f.into_messages_owned()))
//             .flatten()
//     }
// }

// impl Stream for AssignmentStream {
//     type Item = FetchResponseStream;

//     fn poll_next(
//         mut self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Option<Self::Item>> {
//         self.receiver.poll_next_unpin(cx)
//     }
// }

// #[derive(Debug)]
// struct FetchResponseStream {
//     receiver: mpsc::Receiver<FetchResponse>,
// }

// impl FetchResponseStream {
//     pub fn into_message_stream(self) -> impl Stream<Item = KafkaMessage<'static>> {
//         self.map(|f| stream::iter(f.into_messages_owned()))
//             .flatten()
//     }
// }

// impl Stream for FetchResponseStream {
//     type Item = FetchResponse;
//     fn poll_next(
//         mut self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Option<Self::Item>> {
//         self.receiver.poll_next_unpin(cx)
//     }
// }

// #[derive(Debug)]
// struct FetchResponse(Vec<u8>);

// impl FetchResponse {
//     pub fn messages<'a>(&'a self) -> impl Iterator<Item = KafkaMessage<'a>> + 'a {
//         (0..self.0.len()).map(move |pos| KafkaMessage(Cow::Borrowed(&self.0[pos..pos + 1])))
//     }

//     pub fn messages_owned<'a>(&'a self) -> impl Iterator<Item = KafkaMessage<'static>> + 'a {
//         self.0
//             .iter()
//             .map(|data| KafkaMessage(Cow::Owned(vec![*data])))
//     }

//     pub fn into_messages_owned(self) -> impl Iterator<Item = KafkaMessage<'static>> {
//         self.0
//             .into_iter()
//             .map(|data| KafkaMessage(Cow::Owned(vec![data])))
//     }
// }

// #[derive(Debug)]
// struct KafkaMessage<'a>(Cow<'a, [u8]>);

// #[cfg(test)]
// mod test {
//     use super::*;

//     #[tokio::test]
//     async fn this_stuff_works_somehow() {
//         let (mut s, r) = mpsc::channel(1);

//         let assignment = FetchResponseStream { receiver: r };

//         tokio::spawn(async move { s.send(FetchResponse(vec![1, 2, 3])).await.unwrap() });

//         assignment
//             .into_message_stream()
//             .for_each(|m| async move {
//                 //we can send message to other task as it's already owned here
//                 tokio::spawn(async move {
//                     &m;
//                 })
//                 .await
//                 .unwrap();
//             })
//             .await;

//     }

//     #[tokio::test]
//     async fn this_stuff_works_somehow2() {
//         let (mut a_s, a_r) = mpsc::channel(1);

//         tokio::spawn(async move {
//             let (mut s, r) = mpsc::channel(1);
//             let assignment = FetchResponseStream { receiver: r };
//             a_s.send(assignment).await.unwrap();
//             tokio::spawn(async move { s.send(FetchResponse(vec![4, 5, 6])).await.unwrap() });

//             let (mut s, r) = mpsc::channel(1);
//             let assignment = FetchResponseStream { receiver: r };
//             a_s.send(assignment).await.unwrap();
//             tokio::spawn(async move { s.send(FetchResponse(vec![7, 8, 9])).await.unwrap() });
//         });

//         AssignmentStream { receiver: a_r }
//             .into_message_stream()
//             .for_each(|m| async move {
//                 //we can send message to other task as it's already owned here
//                 tokio::spawn(async move {
//                     dbg!(&m);
//                 })
//                 .await
//                 .unwrap();
//             })
//             .await;

//         dbg!("Finished");
//     }

//     #[tokio::test]
//     async fn this_stuff_works_somehow3() {
//         let (mut a_s, a_r) = mpsc::channel(1);

//         tokio::spawn(async move {
//             let (mut s, r) = mpsc::channel(1);
//             let assignment = FetchResponseStream { receiver: r };
//             a_s.send(assignment).await.unwrap();
//             tokio::spawn(async move { s.send(FetchResponse(vec![4, 5, 6])).await.unwrap() });

//             let (mut s, r) = mpsc::channel(1);
//             let assignment = FetchResponseStream { receiver: r };
//             a_s.send(assignment).await.unwrap();
//             tokio::spawn(async move { s.send(FetchResponse(vec![7, 8, 9])).await.unwrap() });
//         });

//         AssignmentStream { receiver: a_r }
//             .into_fetch_stream()
//             .for_each(|f| async move {
//                 //we can send fetch to other task as it's owned
//                 tokio::spawn(async move {
//                     dbg!(&f);
//                     // we can iterate over borrowed messages here, we cannot send them to other thread without detaching
//                     f.messages().for_each(|m| {
//                         dbg!(&m);
//                     });
//                 })
//                 .await
//                 .unwrap();
//             })
//             .await;

//         dbg!("Finished");
//     }
// }
