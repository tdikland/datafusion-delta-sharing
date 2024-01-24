use std::{
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use datafusion::error::Result;
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    parquet::{arrow::async_reader::AsyncFileReader, file::metadata::ParquetMetaData},
    physical_plan::{DisplayAs, ExecutionPlan, Partitioning, RecordBatchStream},
};
use futures::{stream, Stream};
use parquet::{
    arrow::async_reader::{ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder},
    errors::ParquetError,
};

use futures::StreamExt;

use super::ParquetFileReader;

#[derive(Debug)]
pub struct RemoteParquetExec {
    schema: SchemaRef,
    files: Vec<String>,
    sizes: Vec<usize>,
}

impl DisplayAs for RemoteParquetExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ParquetExec")
    }
}

impl ExecutionPlan for RemoteParquetExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        Partitioning::UnknownPartitioning(self.files.len())
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<std::sync::Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let file_stream = FileStream::new(self.files, self.sizes);
        Ok(Box::pin(file_stream))
    }
}

struct FileStream {
    inner: ParquetRecordBatchStream<ParquetFileReader>,
}

impl FileStream {
    async fn new(files: Vec<String>, sizes: Vec<usize>) -> Self {
        let reader = ParquetFileReader::new(files[0].clone(), sizes[0].clone());
        let prbs = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap()
            .build()
            .unwrap();

        Self { inner: prbs }
    }
}

impl Stream for FileStream {
    type Item = Result<datafusion::arrow::array::RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // let n = Pin::new(&mut self.inner).poll_next(cx);
        todo!()
    }
}

impl RecordBatchStream for FileStream {
    fn schema(&self) -> SchemaRef {
        todo!()
    }
}
