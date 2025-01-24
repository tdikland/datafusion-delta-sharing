use std::{any::Any, sync::Arc};

use datafusion::{
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
};

// mod file_stream;
// mod file_reader;
// mod parquet;
// mod schema;



#[derive(Debug)]
pub struct DeltaSharingExec {
    properties: PlanProperties,
}

impl DisplayAs for DeltaSharingExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "DeltaSharingExec"),
            DisplayFormatType::Verbose => write!(f, "DeltaSharingExec"),
        }
    }
}

impl ExecutionPlan for DeltaSharingExec {
    fn name(&self) -> &'static str {
        "DeltaSharingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        // TODO: Implement repartitioned
        Ok(None)
    }

    fn execute(
        &self,
        partition_index: usize,
        ctx: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        // let stream = FileStream::new()
        todo!()
    }
}

// mod exp;
mod reader;
