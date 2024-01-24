// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Execution plan for reading Parquet files

use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use datafusion::datasource::physical_plan::file_stream::{FileOpenFuture, FileOpener, FileStream};
use datafusion::datasource::physical_plan::{
    parquet::page_filter::PagePruningPredicate, DisplayAs, FileMeta, FileScanConfig, SchemaAdapter,
};
use datafusion::{
    config::ConfigOptions,
    datasource::listing::ListingTableUrl,
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
};

use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::ArrowError;
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::{ConvertedType, LogicalType};
use parquet::file::{metadata::ParquetMetaData, properties::WriterProperties};
use parquet::schema::types::ColumnDescriptor;
use tokio::task::JoinSet;

mod metrics;
pub mod page_filter;
mod row_filter;
mod row_groups;
mod statistics;

pub use metrics::ParquetFileMetrics;

/// Execution plan for scanning one or more Parquet partitions
#[derive(Debug, Clone)]
pub struct ParquetExec {
    /// Override for `Self::with_pushdown_filters`. If None, uses
    /// values from base_config
    pushdown_filters: Option<bool>,
    /// Override for `Self::with_reorder_filters`. If None, uses
    /// values from base_config
    reorder_filters: Option<bool>,
    /// Override for `Self::with_enable_page_index`. If None, uses
    /// values from base_config
    enable_page_index: Option<bool>,
    /// Override for `Self::with_enable_bloom_filter`. If None, uses
    /// values from base_config
    enable_bloom_filter: Option<bool>,
    /// Base configuration for this scan
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<LexOrdering>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional predicate for row filtering during parquet scan
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Optional predicate for pruning row groups
    pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Optional predicate for pruning pages
    page_pruning_predicate: Option<Arc<PagePruningPredicate>>,
    /// Optional hint for the size of the parquet metadata
    metadata_size_hint: Option<usize>,
    /// Optional user defined parquet file reader factory
    parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan provided file list and schema.
    pub fn new(
        base_config: FileScanConfig,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metadata_size_hint: Option<usize>,
    ) -> Self {
        debug!(
            "Creating ParquetExec, files: {:?}, projection {:?}, predicate: {:?}, limit: {:?}",
            base_config.file_groups, base_config.projection, predicate, base_config.limit
        );

        let metrics = ExecutionPlanMetricsSet::new();
        let predicate_creation_errors =
            MetricBuilder::new(&metrics).global_counter("num_predicate_creation_errors");

        let file_schema = &base_config.file_schema;
        let pruning_predicate = predicate
            .clone()
            .and_then(|predicate_expr| {
                match PruningPredicate::try_new(predicate_expr, file_schema.clone()) {
                    Ok(pruning_predicate) => Some(Arc::new(pruning_predicate)),
                    Err(e) => {
                        debug!("Could not create pruning predicate for: {e}");
                        predicate_creation_errors.add(1);
                        None
                    }
                }
            })
            .filter(|p| !p.allways_true());

        let page_pruning_predicate = predicate.as_ref().and_then(|predicate_expr| {
            match PagePruningPredicate::try_new(predicate_expr, file_schema.clone()) {
                Ok(pruning_predicate) => Some(Arc::new(pruning_predicate)),
                Err(e) => {
                    debug!(
                        "Could not create page pruning predicate for '{:?}': {}",
                        pruning_predicate, e
                    );
                    predicate_creation_errors.add(1);
                    None
                }
            }
        });

        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();

        Self {
            pushdown_filters: None,
            reorder_filters: None,
            enable_page_index: None,
            enable_bloom_filter: None,
            base_config,
            projected_schema,
            projected_statistics,
            projected_output_ordering,
            metrics,
            predicate,
            pruning_predicate,
            page_pruning_predicate,
            metadata_size_hint,
            parquet_file_reader_factory: None,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    /// Optional predicate.
    pub fn predicate(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.predicate.as_ref()
    }

    /// Optional reference to this parquet scan's pruning predicate
    pub fn pruning_predicate(&self) -> Option<&Arc<PruningPredicate>> {
        self.pruning_predicate.as_ref()
    }

    /// Optional user defined parquet file reader factory.
    ///
    /// `ParquetFileReaderFactory` complements `TableProvider`, It enables users to provide custom
    /// implementation for data access operations.
    ///
    /// If custom `ParquetFileReaderFactory` is provided, then data access operations will be routed
    /// to this factory instead of `ObjectStore`.
    pub fn with_parquet_file_reader_factory(
        mut self,
        parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    ) -> Self {
        self.parquet_file_reader_factory = Some(parquet_file_reader_factory);
        self
    }

    /// If true, any filter [`Expr`]s on the scan will converted to a
    /// [`RowFilter`](parquet::arrow::arrow_reader::RowFilter) in the
    /// `ParquetRecordBatchStream`. These filters are applied by the
    /// parquet decoder to skip unecessairly decoding other columns
    /// which would not pass the predicate. Defaults to false
    ///
    /// [`Expr`]: datafusion_expr::Expr
    pub fn with_pushdown_filters(mut self, pushdown_filters: bool) -> Self {
        self.pushdown_filters = Some(pushdown_filters);
        self
    }

    /// Return the value described in [`Self::with_pushdown_filters`]
    fn pushdown_filters(&self, config_options: &ConfigOptions) -> bool {
        self.pushdown_filters
            .unwrap_or(config_options.execution.parquet.pushdown_filters)
    }

    /// If true, the `RowFilter` made by `pushdown_filters` may try to
    /// minimize the cost of filter evaluation by reordering the
    /// predicate [`Expr`]s. If false, the predicates are applied in
    /// the same order as specified in the query. Defaults to false.
    ///
    /// [`Expr`]: datafusion_expr::Expr
    pub fn with_reorder_filters(mut self, reorder_filters: bool) -> Self {
        self.reorder_filters = Some(reorder_filters);
        self
    }

    /// Return the value described in [`Self::with_reorder_filters`]
    fn reorder_filters(&self, config_options: &ConfigOptions) -> bool {
        self.reorder_filters
            .unwrap_or(config_options.execution.parquet.reorder_filters)
    }

    /// If enabled, the reader will read the page index
    /// This is used to optimise filter pushdown
    /// via `RowSelector` and `RowFilter` by
    /// eliminating unnecessary IO and decoding
    pub fn with_enable_page_index(mut self, enable_page_index: bool) -> Self {
        self.enable_page_index = Some(enable_page_index);
        self
    }

    /// Return the value described in [`Self::with_enable_page_index`]
    fn enable_page_index(&self, config_options: &ConfigOptions) -> bool {
        self.enable_page_index
            .unwrap_or(config_options.execution.parquet.enable_page_index)
    }

    /// If enabled, the reader will read by the bloom filter
    pub fn with_enable_bloom_filter(mut self, enable_bloom_filter: bool) -> Self {
        self.enable_bloom_filter = Some(enable_bloom_filter);
        self
    }

    /// Return the value described in [`Self::with_enable_bloom_filter`]
    fn enable_bloom_filter(&self, config_options: &ConfigOptions) -> bool {
        self.enable_bloom_filter
            .unwrap_or(config_options.execution.parquet.bloom_filter_enabled)
    }
}

impl DisplayAs for ParquetExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let predicate_string = self
                    .predicate
                    .as_ref()
                    .map(|p| format!(", predicate={p}"))
                    .unwrap_or_default();

                let pruning_predicate_string = self
                    .pruning_predicate
                    .as_ref()
                    .map(|pre| format!(", pruning_predicate={}", pre.predicate_expr()))
                    .unwrap_or_default();

                write!(f, "ParquetExec: ")?;
                self.base_config.fmt_as(t, f)?;
                write!(f, "{}{}", predicate_string, pruning_predicate_string,)
            }
        }
    }
}

impl ExecutionPlan for ParquetExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.projected_output_ordering
            .first()
            .map(|ordering| ordering.as_slice())
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new_with_orderings(self.schema(), &self.projected_output_ordering)
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Redistribute files across partitions according to their size
    /// See comments on `get_file_groups_repartitioned()` for more detail.
    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let repartition_file_min_size = config.optimizer.repartition_file_min_size;
        let repartitioned_file_groups_option = FileScanConfig::repartition_file_groups(
            self.base_config.file_groups.clone(),
            target_partitions,
            repartition_file_min_size,
        );

        let mut new_plan = self.clone();
        if let Some(repartitioned_file_groups) = repartitioned_file_groups_option {
            new_plan.base_config.file_groups = repartitioned_file_groups;
        }
        Ok(Some(Arc::new(new_plan)))
    }

    fn execute(
        &self,
        partition_index: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let projection = match self.base_config.file_column_projection_indices() {
            Some(proj) => proj,
            None => (0..self.base_config.file_schema.fields().len()).collect(),
        };

        let parquet_file_reader_factory = self
            .parquet_file_reader_factory
            .as_ref()
            .map(|f| Ok(Arc::clone(f)))
            .unwrap_or_else(|| {
                ctx.runtime_env()
                    .object_store(&self.base_config.object_store_url)
                    .map(|store| {
                        Arc::new(DefaultParquetFileReaderFactory::new(store))
                            as Arc<dyn ParquetFileReaderFactory>
                    })
            })?;

        let config_options = ctx.session_config().options();

        let opener = ParquetOpener {
            partition_index,
            projection: Arc::from(projection),
            batch_size: ctx.session_config().batch_size(),
            limit: self.base_config.limit,
            predicate: self.predicate.clone(),
            pruning_predicate: self.pruning_predicate.clone(),
            page_pruning_predicate: self.page_pruning_predicate.clone(),
            table_schema: self.base_config.file_schema.clone(),
            metadata_size_hint: self.metadata_size_hint,
            metrics: self.metrics.clone(),
            parquet_file_reader_factory,
            pushdown_filters: self.pushdown_filters(config_options),
            reorder_filters: self.reorder_filters(config_options),
            enable_page_index: self.enable_page_index(config_options),
            enable_bloom_filter: self.enable_bloom_filter(config_options),
        };

        let stream = FileStream::new(&self.base_config, partition_index, opener, &self.metrics)?;

        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone())
    }
}

/// Implements [`FileOpener`] for a parquet file
struct ParquetOpener {
    partition_index: usize,
    projection: Arc<[usize]>,
    batch_size: usize,
    limit: Option<usize>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    page_pruning_predicate: Option<Arc<PagePruningPredicate>>,
    table_schema: SchemaRef,
    metadata_size_hint: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    pushdown_filters: bool,
    reorder_filters: bool,
    enable_page_index: bool,
    enable_bloom_filter: bool,
}

impl FileOpener for ParquetOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let file_range = file_meta.range.clone();

        let file_metrics = ParquetFileMetrics::new(
            self.partition_index,
            file_meta.location().as_ref(),
            &self.metrics,
        );

        let reader: Box<dyn AsyncFileReader> = self.parquet_file_reader_factory.create_reader(
            self.partition_index,
            file_meta,
            self.metadata_size_hint,
            &self.metrics,
        )?;

        let batch_size = self.batch_size;
        let projection = self.projection.clone();
        let projected_schema = SchemaRef::from(self.table_schema.project(&projection)?);
        let schema_adapter = SchemaAdapter::new(projected_schema);
        let predicate = self.predicate.clone();
        let pruning_predicate = self.pruning_predicate.clone();
        let page_pruning_predicate = self.page_pruning_predicate.clone();
        let table_schema = self.table_schema.clone();
        let reorder_predicates = self.reorder_filters;
        let pushdown_filters = self.pushdown_filters;
        let enable_page_index =
            should_enable_page_index(self.enable_page_index, &self.page_pruning_predicate);
        let enable_bloom_filter = self.enable_bloom_filter;
        let limit = self.limit;

        Ok(Box::pin(async move {
            let options = ArrowReaderOptions::new().with_page_index(enable_page_index);
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;

            let file_schema = builder.schema().clone();

            let (schema_mapping, adapted_projections) = schema_adapter.map_schema(&file_schema)?;
            // let predicate = predicate.map(|p| reassign_predicate_columns(p, builder.schema(), true)).transpose()?;

            let mask = ProjectionMask::roots(
                builder.parquet_schema(),
                adapted_projections.iter().cloned(),
            );

            // Filter pushdown: evaluate predicates during scan
            if let Some(predicate) = pushdown_filters.then_some(predicate).flatten() {
                let row_filter = row_filter::build_row_filter(
                    &predicate,
                    &file_schema,
                    &table_schema,
                    builder.metadata(),
                    reorder_predicates,
                    &file_metrics,
                );

                match row_filter {
                    Ok(Some(filter)) => {
                        builder = builder.with_row_filter(filter);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!(
                            "Ignoring error building row filter for '{:?}': {}",
                            predicate, e
                        );
                    }
                };
            };

            // Row group pruning by statistics: attempt to skip entire row_groups
            // using metadata on the row groups
            let file_metadata = builder.metadata().clone();
            let predicate = pruning_predicate.as_ref().map(|p| p.as_ref());
            let mut row_groups = row_groups::prune_row_groups_by_statistics(
                &file_schema,
                builder.parquet_schema(),
                file_metadata.row_groups(),
                file_range,
                predicate,
                &file_metrics,
            );

            // Bloom filter pruning: if bloom filters are enabled and then attempt to skip entire row_groups
            // using bloom filters on the row groups
            if enable_bloom_filter && !row_groups.is_empty() {
                if let Some(predicate) = predicate {
                    row_groups = row_groups::prune_row_groups_by_bloom_filters(
                        &mut builder,
                        &row_groups,
                        file_metadata.row_groups(),
                        predicate,
                        &file_metrics,
                    )
                    .await;
                }
            }

            // page index pruning: if all data on individual pages can
            // be ruled using page metadata, rows from other columns
            // with that range can be skipped as well
            if enable_page_index && !row_groups.is_empty() {
                if let Some(p) = page_pruning_predicate {
                    let pruned = p.prune(&row_groups, file_metadata.as_ref(), &file_metrics)?;
                    if let Some(row_selection) = pruned {
                        builder = builder.with_row_selection(row_selection);
                    }
                }
            }

            if let Some(limit) = limit {
                builder = builder.with_limit(limit)
            }

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .with_row_groups(row_groups)
                .build()?;

            let adapted = stream
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .map(move |maybe_batch| {
                    maybe_batch.and_then(|b| schema_mapping.map_batch(b).map_err(Into::into))
                });

            Ok(adapted.boxed())
        }))
    }
}

fn should_enable_page_index(
    enable_page_index: bool,
    page_pruning_predicate: &Option<Arc<PagePruningPredicate>>,
) -> bool {
    enable_page_index
        && page_pruning_predicate.is_some()
        && page_pruning_predicate
            .as_ref()
            .map(|p| p.filter_number() > 0)
            .unwrap_or(false)
}

/// Factory of parquet file readers.
///
/// Provides means to implement custom data access interface.
pub trait ParquetFileReaderFactory: Debug + Send + Sync + 'static {
    /// Provides `AsyncFileReader` over parquet file specified in `FileMeta`
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>>;
}

/// Default parquet reader factory.
#[derive(Debug)]
pub struct DefaultParquetFileReaderFactory {
    store: Arc<dyn ObjectStore>,
}

impl DefaultParquetFileReaderFactory {
    /// Create a factory.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

/// Implements [`AsyncFileReader`] for a parquet file in object storage
pub(crate) struct ParquetFileReader {
    file_metrics: ParquetFileMetrics,
    inner: ParquetObjectReader,
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.file_metrics.bytes_scanned.add(range.end - range.start);
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        let total = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total);
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        self.inner.get_metadata()
    }
}

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics =
            ParquetFileMetrics::new(partition_index, file_meta.location().as_ref(), metrics);
        let store = Arc::clone(&self.store);
        let mut inner = ParquetObjectReader::new(store, file_meta.object_meta);

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint)
        };

        Ok(Box::new(ParquetFileReader {
            inner,
            file_metrics,
        }))
    }
}

/// Executes a query and writes the results to a partitioned Parquet file.
pub async fn plan_to_parquet(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
    writer_properties: Option<WriterProperties>,
) -> Result<()> {
    let path = path.as_ref();
    let parsed = ListingTableUrl::parse(path)?;
    let object_store_url = parsed.object_store();
    let store = task_ctx.runtime_env().object_store(&object_store_url)?;
    let mut join_set = JoinSet::new();
    for i in 0..plan.output_partitioning().partition_count() {
        let plan: Arc<dyn ExecutionPlan> = plan.clone();
        let filename = format!("{}/part-{i}.parquet", parsed.prefix());
        let file = Path::parse(filename)?;
        let propclone = writer_properties.clone();

        let storeref = store.clone();
        let (_, multipart_writer) = storeref.put_multipart(&file).await?;
        let mut stream = plan.execute(i, task_ctx.clone())?;
        join_set.spawn(async move {
            let mut writer =
                AsyncArrowWriter::try_new(multipart_writer, plan.schema(), 10485760, propclone)?;
            while let Some(next_batch) = stream.next().await {
                let batch = next_batch?;
                writer.write(&batch).await?;
            }
            writer
                .close()
                .await
                .map_err(DataFusionError::from)
                .map(|_| ())
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => res?,
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    Ok(())
}

// Convert parquet column schema to arrow data type, and just consider the
// decimal data type.
pub(crate) fn parquet_to_arrow_decimal_type(parquet_column: &ColumnDescriptor) -> Option<DataType> {
    let type_ptr = parquet_column.self_type_ptr();
    match type_ptr.get_basic_info().logical_type() {
        Some(LogicalType::Decimal { scale, precision }) => {
            Some(DataType::Decimal128(precision as u8, scale as i8))
        }
        _ => match type_ptr.get_basic_info().converted_type() {
            ConvertedType::DECIMAL => Some(DataType::Decimal128(
                type_ptr.get_precision() as u8,
                type_ptr.get_scale() as i8,
            )),
            _ => None,
        },
    }
}
