use std::{ops::Range, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    parquet::{arrow::async_reader::AsyncFileReader, file::metadata::ParquetMetaData},
    physical_plan::{DisplayAs, ExecutionPlan, Partitioning},
};

#[derive(Debug)]
pub struct RemoteParquetExec {
    schema: SchemaRef,
    files: Vec<String>,
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
        todo!()
    }
}

struct ParquetFileReader {
    url: String,
}

use bytes::Bytes;
use futures::future::BoxFuture;

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<bytes::Bytes>> {
        let client = reqwest::ClientBuilder::new().build().unwrap();

        // use reqwest::header::ByteRangeSpec;

        // "Range: bytes=0-1023"

        let r = client
            .get(&self.url)
            .header(
                reqwest::header::RANGE,
                format!("{}-{}", range.start, range.end),
            )
            .send();

        Box::pin(r)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        todo!()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        todo!()
    }
}
