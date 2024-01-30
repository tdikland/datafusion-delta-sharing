use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::physical_plan::ParquetExec,
    error::Result,
    physical_expr::Partitioning,
    physical_expr::PhysicalSortExpr,
    physical_plan::{metrics::MetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan, Statistics},
};

use crate::client::action::File;

pub struct DeltaSharingScanBuilder {
    files: Vec<File>,
    projection: Option<Vec<usize>>,
    partition_values: Vec<String>,
}

impl DeltaSharingScanBuilder {
    pub fn new() -> Self {
        Self {
            files: vec![],
            projection: None,
            partition_values: vec![],
        }
    }

    pub fn with_files(mut self, files: Vec<File>) -> Self {
        self.files = files;
        self
    }

    pub fn with_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn build(self) -> DeltaSharingScan {
        todo!()
    }
}

#[derive(Debug)]
pub struct DeltaSharingScan {
    inner: ParquetExec,
}

impl DisplayAs for DeltaSharingScan {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "DeltaSharingScan"),
            DisplayFormatType::Verbose => write!(f, "DeltaSharingScan"),
        }
    }
}

impl ExecutionPlan for DeltaSharingScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.inner.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::new(self.inner.clone())]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        ExecutionPlan::with_new_children(Arc::new(self.inner.clone()), children)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.inner.schema()))
    }
}
