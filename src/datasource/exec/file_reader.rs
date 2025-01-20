use std::{
    future::IntoFuture,
    pin::Pin,
    task::{Context, Poll},
};

use arrow_array::RecordBatch;
use futures::{FutureExt, Stream};

pub struct FileStream<S: ReaderState> {
    reader_state: S,
    state: Box<Inner>,
}

impl FileStream<Idle> {
    pub fn new() -> Self {
        Self {
            reader_state: Idle {},
            state: Box::new(Inner { files: vec![] }),
        }
    }
}

trait ReaderState {}

struct Idle {}

impl ReaderState for Idle {}

enum StreamError {}

pub struct FileListReader {

}

impl FileListReader {
    async fn next(&self) -> Option<Result<RecordBatch, StreamError>> {
        todo!()
    }
}

impl Stream for FileListReader {
    type Item = Result<RecordBatch, StreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
