#[derive(Debug)]
struct DeltaSharingScan {
    inner: ParquetExec,
}

impl DisplayAs for DeltaSharingScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        _f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for DeltaSharingScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        todo!()
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        todo!()
    }
}
