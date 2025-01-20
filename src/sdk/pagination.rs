// use std::{
//     future::Future,
//     pin::Pin,
//     task::{Context, Poll},
// };

// use futures::Stream;
// use pin_project_lite::pin_project;

// use crate::{model::Share, rest};

// pub enum ClientError {}

// pub struct Client {
//     inner: rest::DeltaSharingRestClient,
// }

// impl Client {
//     pub fn list_shares(&self) -> impl Stream<Item = Result<Share, ClientError>> {
//         Paginated::<_, Share>::new(|max_results, page_token| async {
//             let response = self.inner.list_shares_paginated(None, None).await?;
//             response
//         })
//     }
// }

// pub struct Pagination {
//     max_results: Option<u32>,
//     page_token: Option<String>,
// }

// pin_project! {
//     pub struct Paginated<F, Fut, I> {
//         f: F,
//         #[pin]
//         fut: Option<Fut>,
//         max_results: Option<u32>,
//         next_page_token: Option<String>,
//         items: Vec<I>,
//         done: bool,
//     }
// }

// impl<F, Fut, I> Paginated<F, Fut, I>
// where
//     F: Clone + FnOnce(Pagination) -> Fut + Send,
//     Fut: Future<Output = (I, Pagination)> + Send,
// {
//     pub fn new(f: F) -> Self {
//         Self {
//             f,
//             fut: None,
//             max_results: None,
//             next_page_token: None,
//             items: vec![],
//             done: false,
//         }
//     }

//     pub fn with_max_results(&mut self, max_results: Option<u32>) -> &mut Self {
//         self.max_results = max_results;
//         self
//     }

//     pub fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<I>> {
//         if self.done {
//             return Poll::Ready(None);
//         }

//         let this = self.project();
//         if !this.items.is_empty() {
//             return Poll::Ready(this.items.pop());
//         }

//         if this.fut.is_none() {
//             let new_fut = (this.f)(Pagination {
//                 max_results: *this.max_results,
//                 page_token: this.next_page_token.clone(),
//             });
//             *this.fut.as_mut() = Some(new_fut);
//         }

//         todo!()
//     }
// }

// impl<F, Fut, I> Stream for Paginated<F, I>
// where
//     F: FnOnce(Pagination) -> Fut + Send,
//     Fut: Future<Output = (I, Pagination)> + Send,
// {
//     type Item = Result<I, ClientError>;

//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         // self.poll_next(cx).map(|_| self.items.pop())
//         todo!()
//     }
// }

// pub struct Pagination {
//     max_results: Option<u32>,
//     page_token: Option<String>,
// }

// pub struct Paginated<F, I> {
//     f: F,
//     max_results: Option<u32>,
//     next_page_token: Option<String>,
//     items: Vec<I>,
//     done: bool,
// }

// impl<F, Fut, R> Paginated<F, R>
// where
//     F: FnOnce(Option<u32>, Option<String>) -> Fut + Send,
//     Fut: Future<Output = R> + Send,
//     R: Unpin,
// {
//     pub fn new(f: F) -> Self {
//         Self {
//             f,
//             max_results: None,
//             next_page_token: None,
//             items: vec![],
//             done: false,
//         }
//     }

//     pub fn with_max_results(&mut self, max_results: Option<u32>) -> &mut Self {
//         self.max_results = max_results;
//         self
//     }

//     // pub fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<()>> {
//     //     if self.done {
//     //         return Poll::Ready(None);
//     //     }

//     //     let response =
//     //         std::pin::pin!((self.f)(self.max_results, self.next_page_token.clone())).poll(cx);
//     //     match response {
//     //         Poll::Ready(response) => {
//     //             self.items.push(response);
//     //             self.next_page_token = None;
//     //             self.done = true;
//     //             Poll::Ready(Some(()))
//     //         }
//     //         Poll::Pending => Poll::Pending,
//     //     }
//     // }
// }

// impl<F, I> Stream for Paginated<F, I>
// where
//     F: FnOnce(Option<u32>, Option<String>) -> Pin<Box<dyn Future<Output = I>>>,
// {
//     type Item = Result<I, ClientError>;

//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         // self.poll_next(cx).map(|_| self.items.pop())
//         todo!()
//     }
// }
