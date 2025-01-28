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
    physical_plan::metrics::ExecutionPlanMetricsSet,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
};
use futures::{future::BoxFuture, stream, FutureExt, Stream, StreamExt, TryStreamExt};
use http::header::RANGE;
use parquet::{
    arrow::{
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
        async_reader::ParquetRecordBatchStream,
        ParquetRecordBatchStreamBuilder,
    },
    errors::ParquetError,
    file::{
        metadata::{ParquetMetaData, ParquetMetaDataReader},
        reader::SerializedFileReader,
    },
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
        let schema = self.schema.clone();
        let files = self.files.clone();
        let client = self.client.clone();

        let stream = stream::iter(files.into_iter().map(move |file| {
            let client = client.clone();
            async move {
                // fetch the file from the interweb
                let reader = client
                    .get(file.url)
                    .send()
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                let metadata = ArrowReaderMetadata::load(&reader, Default::default())?;
                let parquet_schema = metadata.schema();

                let options = ArrowReaderOptions::new();
                let mut builder =
                    ParquetRecordBatchReaderBuilder::try_new_with_options(reader, options)?;
                let reader = builder.with_batch_size(1024).build()?;
                let stream = futures::stream::iter(reader);
                Ok::<_, DataFusionError>(
                    stream
                        .boxed()
                        .map_err(|e| DataFusionError::Execution(e.to_string())),
                )
            }
        }))
        .buffer_unordered(10)
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            Box::pin(stream),
        )))
    }
}
