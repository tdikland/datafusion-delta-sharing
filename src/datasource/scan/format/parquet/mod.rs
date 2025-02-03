use core::{fmt, panic};
use std::sync::Arc;

use arrow_array::StringArray;
use arrow_schema::SchemaRef;

use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
    scalar::ScalarValue,
};
use delta_kernel::scan;
use futures::{stream, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use reqwest::Client;

use datafusion::datasource::{
    listing::PartitionedFile,
    physical_plan::{FileScanConfig, ParquetExec},
};

use crate::{
    datasource::schema::{LogicalScanSchema, LogicalTableSchema},
    model::action::parquet::File,
};

pub struct SharedParquetExec {
    client: Client,
    schema: SchemaRef,
    properties: PlanProperties,
    files: Vec<File>,
    partition_columns: Vec<String>,
    table_schema: LogicalTableSchema,
    scan_schema: LogicalScanSchema,
}

impl SharedParquetExec {
    pub fn new(
        schema: SchemaRef,
        files: Vec<File>,
        partition_columns: Vec<String>,
        logical_table_schema: LogicalTableSchema,
        scan_schema: LogicalScanSchema,
    ) -> Self {
        let client = Client::builder().build().unwrap();
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
            partition_columns,
            table_schema: logical_table_schema,
            scan_schema,
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
        let table_schema = self.table_schema.clone();
        tracing::warn!(schema = ?schema, table_schema=?table_schema, "executing SharedParquetExec");

        let stream = stream::iter(files.into_iter().map(move |file| {
            let client = client.clone();
            let table_schema = table_schema.clone();
            tracing::warn!(url = file.url, "reading url FILE");
            async move {
                // fetch the file from the interweb
                let reader = client
                    .get(file.url.clone())
                    .send()
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                let metadata = ArrowReaderMetadata::load(&reader, Default::default())?;
                // let parquet_schema = metadata.schema();

                let options = ArrowReaderOptions::new();
                let builder =
                    ParquetRecordBatchReaderBuilder::try_new_with_options(reader, options)?;
                let reader = builder.with_batch_size(1024).build()?;

                let iter = reader.into_iter().map(move |rb| {
                    let table_schema = table_schema.clone();
                    match rb {
                        Ok(rb) => {
                            let schema = table_schema.as_arrow().clone();
                            let mut cols = rb.columns().to_vec();
                            for (partition_col, partition_value) in file.partition_values() {
                                tracing::warn!(
                                    schema = ?schema,
                                    partition_col = partition_col,
                                    partition_value = partition_value,
                                    "adding partition column"
                                );

                                let (col_idx, field) =
                                    schema.column_with_name(&partition_col).unwrap();
                                let val = match field.data_type() {
                                    arrow_schema::DataType::Utf8
                                    | arrow_schema::DataType::LargeUtf8 => {
                                        Arc::new(StringArray::from_iter_values(
                                            std::iter::repeat(partition_value.to_string())
                                                .take(rb.num_rows()),
                                        ))
                                    }
                                    _ => panic!("unsupported partition column type"),
                                };
                                cols.insert(col_idx, val);
                            }
                            let res = arrow::record_batch::RecordBatch::try_new(
                                table_schema.as_arrow(),
                                cols,
                            )
                            .unwrap();
                            Ok(res)
                        }
                        Err(e) => Err(e),
                    }
                });

                let stream = futures::stream::iter(iter);
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
