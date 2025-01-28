use core::fmt;
use std::{
    future::Future,
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
    time::Duration,
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::{
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use futures::{future::BoxFuture, stream, FutureExt, Stream, StreamExt};
use http::header::RANGE;
use parquet::{
    arrow::{
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
        async_reader::{AsyncFileReader, ParquetRecordBatchStream},
        ParquetRecordBatchStreamBuilder,
    },
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use reqwest::Client;

use crate::model::action::parquet::File;

pub struct SharedParquetExec {
    client: Client,
    schema: SchemaRef,
    properties: PlanProperties,
    files: Vec<File>,
}

impl SharedParquetExec {
    pub fn new(schema: SchemaRef, files: Vec<File>) -> Self {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(2))
            .read_timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let props = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            client,
            schema,
            properties: props,
            files,
        }
    }
}

impl fmt::Debug for SharedParquetExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedParquetExec")
            .field("client", &self.client)
            .finish()
    }
}

impl DisplayAs for SharedParquetExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "SharedParquetExec"),
            DisplayFormatType::Verbose => write!(f, "SharedParquetExec"),
        }
    }
}

impl ExecutionPlan for SharedParquetExec {
    fn name(&self) -> &str {
        "SharedParquetExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        Ok(Box::pin(SignedFileStream::new(
            self.client.clone(),
            self.schema.clone(),
            self.files.clone(),
        )))
    }
}

struct SignedObjectMeta {
    presigned_url: String,
    file_size: usize,
    last_modified: DateTime<Utc>,
}

struct SignedFileStream<'a> {
    client: Client,
    schema: SchemaRef,
    files: Vec<SignedObjectMeta>,
    current_stream: Option<DataFile<'a>>,
}

impl SignedFileStream<'_> {
    fn new(client: Client, schema: SchemaRef, files: Vec<File>) -> Self {
        Self {
            client,
            schema,
            files: files
                .into_iter()
                .map(|file| SignedObjectMeta {
                    presigned_url: file.url().to_string(),
                    file_size: file.size() as usize,
                    last_modified: Utc::now(),
                })
                .collect(),
            current_stream: None,
        }
    }
}

impl Stream for SignedFileStream<'_> {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.current_stream.is_none() && self.files.is_empty() {
            return Poll::Ready(None);
        }

        loop {
            if let Some(stream) = self.current_stream.as_mut() {
                let item = ready!(stream.poll_next_unpin(cx));
                match item {
                    Some(Ok(batch)) => return Poll::Ready(Some(Ok(batch))),
                    Some(Err(e)) => {
                        return Poll::Ready(Some(Err(DataFusionError::Execution(e.to_string()))))
                    }
                    None => self.current_stream = None,
                }
            } else {
                if let Some(file) = self.files.pop() {
                    let data =
                        DataFile::new(self.client.clone(), file.presigned_url, file.file_size);
                    self.current_stream = Some(data);
                } else {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for SignedFileStream<'_> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub struct SignedObjectReader {
    client: Client,
    signed_url: String,
    file_size: usize,
}

impl SignedObjectReader {
    pub fn new(client: Client, url: String, size: usize) -> Self {
        Self {
            client,
            signed_url: url,
            file_size: size,
        }
    }

    pub async fn get_range(&self, range: Range<usize>) -> Result<Bytes, ParquetError> {
        self.client
            .get(&self.signed_url)
            .header(RANGE, format!("bytes={}-{}", range.start, range.end))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .map_err(|e| ParquetError::General(e.to_string()))
    }
}

impl AsyncFileReader for SignedObjectReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        async move { self.get_range(range).await }.boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>, ParquetError>> {
        async move {
            let file_size = self.file_size;
            let metadata = ParquetMetaDataReader::new()
                .load_and_finish(self, file_size)
                .await
                .unwrap();
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

enum FileState<'a> {
    Start,
    Opening {
        meta: BoxFuture<'a, Result<ArrowReaderMetadata, ParquetError>>,
    },
    Scanning {
        stream: ParquetRecordBatchStream<SignedObjectReader>,
    },
    End,
}

struct DataFile<'a> {
    reader: SignedObjectReader,
    opts: ArrowReaderOptions,
    state: FileState<'a>,
}

impl DataFile<'_> {
    pub fn new(client: Client, signed_url: String, size: usize) -> Self {
        let reader = SignedObjectReader::new(client, signed_url, size);
        let opts = ArrowReaderOptions::default();

        Self {
            reader,
            opts,
            state: FileState::Start,
        }
    }
}

impl Stream for DataFile<'_> {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                FileState::Start => {
                    let read_metadata_fut =
                        ArrowReaderMetadata::load_async(&mut self.reader, self.opts.clone())
                            .boxed();
                    self.state = FileState::Opening {
                        meta: read_metadata_fut,
                    };
                }
                FileState::Opening { ref mut meta } => {
                    let metadata = ready!(meta.poll_unpin(cx));
                    let stream = ParquetRecordBatchStreamBuilder::new_with_metadata(
                        self.reader.clone(),
                        metadata.unwrap(),
                    )
                    .build()
                    .unwrap();
                    self.state = FileState::Scanning { stream };
                }
                FileState::Scanning { ref mut stream } => {
                    let item = ready!(stream.poll_next_unpin(cx));
                    match item {
                        Some(Ok(batch)) => return Poll::Ready(Some(Ok(batch))),
                        Some(Err(e)) => {
                            return Poll::Ready(Some(Err(DataFusionError::Execution(
                                e.to_string(),
                            ))))
                        }
                        None => self.state = FileState::End,
                    }
                }
                FileState::End => return Poll::Ready(None),
            }
        }
    }
}
