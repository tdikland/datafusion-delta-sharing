use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::{
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_plan::ExecutionPlan,
};
use futures::Stream;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;

pub struct SharedParquetExec {
    client: Client,
}

impl ExecutionPlan for SharedParquetExec {
    fn name(&self) -> &str {
        "SharedParquetExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        todo!()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        Ok(Box::pin(SignedFileStream::new(self.client.clone())))
    }
}

struct ObjectMetadata {
    presigned_url: String,
}

struct SignedFileStream {
    client: Client,
    schema: SchemaRef,
    files: Vec<ObjectMetadata>,
}

impl SignedFileStream {
    fn new(client: Client) -> Self {
        Self {
            client,
            schema: todo!(),
            files: Vec::new(),
        }
    }

    async fn ne(&self) -> () {
        let first_file = self.files[0];

        let file = self
            .client
            .get(first_file.presigned_url)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let md = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
        let parquet_schema = metadata.schema();
        let (indicies, requested_ordering) =
            get_requested_indices(self.schema, parquet_schema).unwrap();

        let options = ArrowReaderOptions::new();
        let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(reader, options)?;
        if let Some(mask) = generate_mask(
            &self.schema,
            parquet_schema,
            builder.parquet_schema(),
            &indicies,
        ) {
            builder = builder.with_projection(mask)
        }

        let reader = builder.with_batch_size(batch_size).build()?;
        let stream = futures::stream::iter(reader);
        let stream = stream.map(move |rbr| {
            // re-order each batch if needed
            rbr.map_err(Error::Arrow).and_then(|rb| {
                reorder_struct_array(rb.into(), &requested_ordering).map(Into::into)
            })
        });
    }
}

impl Stream for SignedFileStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

impl RecordBatchStream for SignedFileStream {
    fn schema(&self) -> arrow_schema::SchemaRef {
        todo!()
    }
}
