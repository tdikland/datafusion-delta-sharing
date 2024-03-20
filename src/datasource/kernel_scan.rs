use std::{
    any::Any,
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{compute::filter_record_batch, record_batch::RecordBatch};
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::Result,
    execution::RecordBatchStream,
    physical_expr::{Partitioning, PhysicalSortExpr},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan},
};
use delta_kernel::{scan::Scan, simple_client::data::SimpleData, EngineInterface};
use futures::Stream;
use pin_project_lite::pin_project;
use tracing::debug;

pub struct DeltaScan {
    projected_schema: SchemaRef,
    inner: Scan,
    engine: Arc<dyn EngineInterface + Send + Sync>,
}

impl DeltaScan {
    pub fn new(
        projected_schema: SchemaRef,
        inner: Scan,
        engine: Arc<dyn EngineInterface + Send + Sync>,
    ) -> Self {
        Self {
            projected_schema,
            inner,
            engine,
        }
    }
}

impl Debug for DeltaScan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaSharingExec")
    }
}

impl DisplayAs for DeltaScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaSharingExec")
    }
}

impl ExecutionPlan for DeltaScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        let schema = self.projected_schema.clone();
        let mut result = self.inner.execute(&*self.engine).unwrap();

        let records = result.into_iter().map(|res| {
            // debug!(res = ?res.raw_data, "received record batch from delta sharing");
            let batch: RecordBatch = res
                .raw_data
                .unwrap()
                .into_any()
                .downcast::<SimpleData>()
                .unwrap()
                .record_batch()
                .clone();
            let filtered = match &res.mask {
                Some(mask) => filter_record_batch(&batch, &(mask.clone().into())).unwrap(),
                None => batch,
            };
            Ok(filtered)
        });
        let stream = futures::stream::iter(records);
        let delta_stream = DeltaRecordBatchStream::new(stream, schema);

        Ok(Box::pin(delta_stream))
    }
}



pin_project! {
    struct DeltaRecordBatchStream<S> {
        #[pin]
        inner: S,
        schema: SchemaRef,
    }
}

impl<S: Stream<Item = Result<RecordBatch>>> DeltaRecordBatchStream<S> {
    fn new(inner: S, schema: SchemaRef) -> Self {
        Self { inner, schema }
    }
}

impl<S: Stream<Item = Result<RecordBatch>>> Stream for DeltaRecordBatchStream<S> {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx)
    }
}

impl<S: Stream<Item = Result<RecordBatch>>> RecordBatchStream for DeltaRecordBatchStream<S> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
