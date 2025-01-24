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

//! [`ParquetExec`] Execution plan for reading Parquet files

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::file_stream::FileStream;
use datafusion::datasource::physical_plan::{
    parquet::page_filter::PagePruningAccessPlanFilter, DisplayAs, FileGroupPartitioner,
    FileScanConfig,
};
use datafusion::{
    config::{ConfigOptions, TableParquetOptions},
    error::Result,
    execution::context::TaskContext,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
        Statistics,
    },
};

use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

use itertools::Itertools;
use log::debug;

mod access_plan;
mod metrics;
mod opener;
mod page_filter;
mod reader;
mod row_filter;
mod row_group_filter;
mod writer;

use crate::datasource::schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapterFactory};
pub use access_plan::{ParquetAccessPlan, RowGroupAccess};
pub use metrics::ParquetFileMetrics;
use opener::ParquetOpener;
pub use reader::{DefaultParquetFileReaderFactory, ParquetFileReaderFactory};
pub use row_filter::can_expr_be_pushed_down_with_schemas;
pub use writer::plan_to_parquet;


#[derive(Debug, Clone)]
pub struct ParquetExec {
    /// Base configuration for this scan
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional predicate for row filtering during parquet scan
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Optional predicate for pruning row groups (derived from `predicate`)
    pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Optional predicate for pruning pages (derived from `predicate`)
    page_pruning_predicate: Option<Arc<PagePruningAccessPlanFilter>>,
    /// Optional hint for the size of the parquet metadata
    metadata_size_hint: Option<usize>,
    /// Optional user defined parquet file reader factory
    parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
    /// Cached plan properties such as equivalence properties, ordering, partitioning, etc.
    cache: PlanProperties,
    /// Options for reading Parquet files
    table_parquet_options: TableParquetOptions,
    /// Optional user defined schema adapter
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl From<ParquetExec> for ParquetExecBuilder {
    fn from(exec: ParquetExec) -> Self {
        exec.into_builder()
    }
}

/// [`ParquetExecBuilder`], builder for [`ParquetExec`].
///
/// See example on [`ParquetExec`].
pub struct ParquetExecBuilder {
    file_scan_config: FileScanConfig,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    metadata_size_hint: Option<usize>,
    table_parquet_options: TableParquetOptions,
    parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl ParquetExecBuilder {
    /// Create a new builder to read the provided file scan configuration
    pub fn new(file_scan_config: FileScanConfig) -> Self {
        Self::new_with_options(file_scan_config, TableParquetOptions::default())
    }

    /// Create a new builder to read the data specified in the file scan
    /// configuration with the provided `TableParquetOptions`.
    pub fn new_with_options(
        file_scan_config: FileScanConfig,
        table_parquet_options: TableParquetOptions,
    ) -> Self {
        Self {
            file_scan_config,
            predicate: None,
            metadata_size_hint: None,
            table_parquet_options,
            parquet_file_reader_factory: None,
            schema_adapter_factory: None,
        }
    }

    /// Update the list of files groups to read
    pub fn with_file_groups(mut self, file_groups: Vec<Vec<PartitionedFile>>) -> Self {
        self.file_scan_config.file_groups = file_groups;
        self
    }

    /// Set the filter predicate when reading.
    ///
    /// See the "Predicate Pushdown" section of the [`ParquetExec`] documentation
    /// for more details.
    pub fn with_predicate(mut self, predicate: Arc<dyn PhysicalExpr>) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Set the metadata size hint
    ///
    /// This value determines how many bytes at the end of the file the default
    /// [`ParquetFileReaderFactory`] will request in the initial IO. If this is
    /// too small, the ParquetExec will need to make additional IO requests to
    /// read the footer.
    pub fn with_metadata_size_hint(mut self, metadata_size_hint: usize) -> Self {
        self.metadata_size_hint = Some(metadata_size_hint);
        self
    }

    /// Set the options for controlling how the ParquetExec reads parquet files.
    ///
    /// See also [`Self::new_with_options`]
    pub fn with_table_parquet_options(
        mut self,
        table_parquet_options: TableParquetOptions,
    ) -> Self {
        self.table_parquet_options = table_parquet_options;
        self
    }

    /// Set optional user defined parquet file reader factory.
    ///
    /// You can use [`ParquetFileReaderFactory`] to more precisely control how
    /// data is read from parquet files (e.g. skip re-reading metadata, coalesce
    /// I/O operations, etc).
    ///
    /// The default reader factory reads directly from an [`ObjectStore`]
    /// instance using individual I/O operations for the footer and each page.
    ///
    /// If a custom `ParquetFileReaderFactory` is provided, then data access
    /// operations will be routed to this factory instead of [`ObjectStore`].
    ///
    /// [`ObjectStore`]: object_store::ObjectStore
    pub fn with_parquet_file_reader_factory(
        mut self,
        parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    ) -> Self {
        self.parquet_file_reader_factory = Some(parquet_file_reader_factory);
        self
    }

    /// Set optional schema adapter factory.
    ///
    /// [`SchemaAdapterFactory`] allows user to specify how fields from the
    /// parquet file get mapped to that of the table schema.  The default schema
    /// adapter uses arrow's cast library to map the parquet fields to the table
    /// schema.
    pub fn with_schema_adapter_factory(
        mut self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Self {
        self.schema_adapter_factory = Some(schema_adapter_factory);
        self
    }

    /// Convenience: build an `Arc`d `ParquetExec` from this builder
    pub fn build_arc(self) -> Arc<ParquetExec> {
        Arc::new(self.build())
    }

    /// Build a [`ParquetExec`]
    #[must_use]
    pub fn build(self) -> ParquetExec {
        let Self {
            file_scan_config,
            predicate,
            metadata_size_hint,
            table_parquet_options,
            parquet_file_reader_factory,
            schema_adapter_factory,
        } = self;

        let base_config = file_scan_config;
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
                match PruningPredicate::try_new(predicate_expr, Arc::clone(file_schema)) {
                    Ok(pruning_predicate) => Some(Arc::new(pruning_predicate)),
                    Err(e) => {
                        debug!("Could not create pruning predicate for: {e}");
                        predicate_creation_errors.add(1);
                        None
                    }
                }
            })
            .filter(|p| !p.always_true());

        let page_pruning_predicate = predicate
            .as_ref()
            .map(|predicate_expr| {
                PagePruningAccessPlanFilter::new(predicate_expr, Arc::clone(file_schema))
            })
            .map(Arc::new);

        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();

        let cache = ParquetExec::compute_properties(
            projected_schema,
            &projected_output_ordering,
            &base_config,
        );
        ParquetExec {
            base_config,
            projected_statistics,
            metrics,
            predicate,
            pruning_predicate,
            page_pruning_predicate,
            metadata_size_hint,
            parquet_file_reader_factory,
            cache,
            table_parquet_options,
            schema_adapter_factory,
        }
    }
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan provided file list and schema.
    #[deprecated(
        since = "39.0.0",
        note = "use `ParquetExec::builder` or `ParquetExecBuilder`"
    )]
    pub fn new(
        base_config: FileScanConfig,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metadata_size_hint: Option<usize>,
        table_parquet_options: TableParquetOptions,
    ) -> Self {
        let mut builder = ParquetExecBuilder::new_with_options(base_config, table_parquet_options);
        if let Some(predicate) = predicate {
            builder = builder.with_predicate(predicate);
        }
        if let Some(metadata_size_hint) = metadata_size_hint {
            builder = builder.with_metadata_size_hint(metadata_size_hint);
        }
        builder.build()
    }

    /// Return a [`ParquetExecBuilder`].
    ///
    /// See example on [`ParquetExec`] and [`ParquetExecBuilder`] for specifying
    /// parquet table options.
    pub fn builder(file_scan_config: FileScanConfig) -> ParquetExecBuilder {
        ParquetExecBuilder::new(file_scan_config)
    }

    /// Convert this `ParquetExec` into a builder for modification
    pub fn into_builder(self) -> ParquetExecBuilder {
        // list out fields so it is clear what is being dropped
        // (note the fields which are dropped are re-created as part of calling
        // `build` on the builder)
        let Self {
            base_config,
            projected_statistics: _,
            metrics: _,
            predicate,
            pruning_predicate: _,
            page_pruning_predicate: _,
            metadata_size_hint,
            parquet_file_reader_factory,
            cache: _,
            table_parquet_options,
            schema_adapter_factory,
        } = self;
        ParquetExecBuilder {
            file_scan_config: base_config,
            predicate,
            metadata_size_hint,
            table_parquet_options,
            parquet_file_reader_factory,
            schema_adapter_factory,
        }
    }

    /// [`FileScanConfig`] that controls this scan (such as which files to read)
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    /// Options passed to the parquet reader for this scan
    pub fn table_parquet_options(&self) -> &TableParquetOptions {
        &self.table_parquet_options
    }

    /// Optional predicate.
    pub fn predicate(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.predicate.as_ref()
    }

    /// Optional reference to this parquet scan's pruning predicate
    pub fn pruning_predicate(&self) -> Option<&Arc<PruningPredicate>> {
        self.pruning_predicate.as_ref()
    }

    /// return the optional file reader factory
    pub fn parquet_file_reader_factory(&self) -> Option<&Arc<dyn ParquetFileReaderFactory>> {
        self.parquet_file_reader_factory.as_ref()
    }

    /// Optional user defined parquet file reader factory.
    pub fn with_parquet_file_reader_factory(
        mut self,
        parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    ) -> Self {
        self.parquet_file_reader_factory = Some(parquet_file_reader_factory);
        self
    }

    /// return the optional schema adapter factory
    pub fn schema_adapter_factory(&self) -> Option<&Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.as_ref()
    }

    /// Optional schema adapter factory.
    ///
    /// See documentation on [`ParquetExecBuilder::with_schema_adapter_factory`]
    pub fn with_schema_adapter_factory(
        mut self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Self {
        self.schema_adapter_factory = Some(schema_adapter_factory);
        self
    }

    /// If true, the predicate will be used during the parquet scan.
    /// Defaults to false
    ///
    /// [`Expr`]: datafusion_expr::Expr
    pub fn with_pushdown_filters(mut self, pushdown_filters: bool) -> Self {
        self.table_parquet_options.global.pushdown_filters = pushdown_filters;
        self
    }

    /// Return the value described in [`Self::with_pushdown_filters`]
    fn pushdown_filters(&self) -> bool {
        self.table_parquet_options.global.pushdown_filters
    }

    /// If true, the `RowFilter` made by `pushdown_filters` may try to
    /// minimize the cost of filter evaluation by reordering the
    /// predicate [`Expr`]s. If false, the predicates are applied in
    /// the same order as specified in the query. Defaults to false.
    ///
    /// [`Expr`]: datafusion_expr::Expr
    pub fn with_reorder_filters(mut self, reorder_filters: bool) -> Self {
        self.table_parquet_options.global.reorder_filters = reorder_filters;
        self
    }

    /// Return the value described in [`Self::with_reorder_filters`]
    fn reorder_filters(&self) -> bool {
        self.table_parquet_options.global.reorder_filters
    }

    /// If enabled, the reader will read the page index
    /// This is used to optimize filter pushdown
    /// via `RowSelector` and `RowFilter` by
    /// eliminating unnecessary IO and decoding
    pub fn with_enable_page_index(mut self, enable_page_index: bool) -> Self {
        self.table_parquet_options.global.enable_page_index = enable_page_index;
        self
    }

    /// Return the value described in [`Self::with_enable_page_index`]
    fn enable_page_index(&self) -> bool {
        self.table_parquet_options.global.enable_page_index
    }

    /// If enabled, the reader will read by the bloom filter
    pub fn with_bloom_filter_on_read(mut self, bloom_filter_on_read: bool) -> Self {
        self.table_parquet_options.global.bloom_filter_on_read = bloom_filter_on_read;
        self
    }

    /// If enabled, the writer will write by the bloom filter
    pub fn with_bloom_filter_on_write(mut self, enable_bloom_filter_on_write: bool) -> Self {
        self.table_parquet_options.global.bloom_filter_on_write = enable_bloom_filter_on_write;
        self
    }

    /// Return the value described in [`Self::with_bloom_filter_on_read`]
    fn bloom_filter_on_read(&self) -> bool {
        self.table_parquet_options.global.bloom_filter_on_read
    }

    fn output_partitioning_helper(file_config: &FileScanConfig) -> Partitioning {
        Partitioning::UnknownPartitioning(file_config.file_groups.len())
    }

    /// This function creates the cache object that stores the plan properties such as schema,
    /// equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        file_config: &FileScanConfig,
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new_with_orderings(schema, orderings),
            Self::output_partitioning_helper(file_config), // Output Partitioning
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    /// Updates the file groups to read and recalculates the output partitioning
    ///
    /// Note this function does not update statistics or other properties
    /// that depend on the file groups.
    fn with_file_groups_and_update_partitioning(
        mut self,
        file_groups: Vec<Vec<PartitionedFile>>,
    ) -> Self {
        self.base_config.file_groups = file_groups;
        // Changing file groups may invalidate output partitioning. Update it also
        let output_partitioning = Self::output_partitioning_helper(&self.base_config);
        self.cache = self.cache.with_partitioning(output_partitioning);
        self
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
                    .map(|pre| {
                        let mut guarantees = pre
                            .literal_guarantees()
                            .iter()
                            .map(|item| format!("{}", item))
                            .collect_vec();
                        guarantees.sort();
                        format!(
                            ", pruning_predicate={}, required_guarantees=[{}]",
                            pre.predicate_expr(),
                            guarantees.join(", ")
                        )
                    })
                    .unwrap_or_default();

                write!(f, "ParquetExec: ")?;
                self.base_config.fmt_as(t, f)?;
                write!(f, "{}{}", predicate_string, pruning_predicate_string,)
            }
        }
    }
}

impl ExecutionPlan for ParquetExec {
    fn name(&self) -> &'static str {
        "ParquetExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Redistribute files across partitions according to their size
    /// See comments on [`FileGroupPartitioner`] for more detail.
    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let repartition_file_min_size = config.optimizer.repartition_file_min_size;
        let repartitioned_file_groups_option = FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_repartition_file_min_size(repartition_file_min_size)
            .with_preserve_order_within_groups(self.properties().output_ordering().is_some())
            .repartition_file_groups(&self.base_config.file_groups);

        let mut new_plan = self.clone();
        if let Some(repartitioned_file_groups) = repartitioned_file_groups_option {
            new_plan = new_plan.with_file_groups_and_update_partitioning(repartitioned_file_groups);
        }
        Ok(Some(Arc::new(new_plan)))
    }

    fn execute(
        &self,
        partition_index: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let projection = self
            .base_config
            .file_column_projection_indices()
            .unwrap_or_else(|| (0..self.base_config.file_schema.fields().len()).collect());

        let parquet_file_reader_factory = self
            .parquet_file_reader_factory
            .as_ref()
            .map(|f| Ok(Arc::clone(f)))
            .unwrap_or_else(|| {
                ctx.runtime_env()
                    .object_store(&self.base_config.object_store_url)
                    .map(|store| Arc::new(DefaultParquetFileReaderFactory::new(store)) as _)
            })?;

        let schema_adapter_factory = self
            .schema_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultSchemaAdapterFactory));

        let opener = ParquetOpener {
            partition_index,
            projection: Arc::from(projection),
            batch_size: ctx.session_config().batch_size(),
            limit: self.base_config.limit,
            predicate: self.predicate.clone(),
            pruning_predicate: self.pruning_predicate.clone(),
            page_pruning_predicate: self.page_pruning_predicate.clone(),
            table_schema: Arc::clone(&self.base_config.file_schema),
            metadata_size_hint: self.metadata_size_hint,
            metrics: self.metrics.clone(),
            parquet_file_reader_factory,
            pushdown_filters: self.pushdown_filters(),
            reorder_filters: self.reorder_filters(),
            enable_page_index: self.enable_page_index(),
            enable_bloom_filter: self.bloom_filter_on_read(),
            schema_adapter_factory,
        };

        let stream = FileStream::new(&self.base_config, partition_index, opener, &self.metrics)?;

        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        // When filters are pushed down, we have no way of knowing the exact statistics.
        // Note that pruning predicate is also a kind of filter pushdown.
        // (bloom filters use `pruning_predicate` too)
        let stats = if self.pruning_predicate.is_some()
            || self.page_pruning_predicate.is_some()
            || (self.predicate.is_some() && self.pushdown_filters())
        {
            self.projected_statistics.clone().to_inexact()
        } else {
            self.projected_statistics.clone()
        };
        Ok(stats)
    }

    fn fetch(&self) -> Option<usize> {
        self.base_config.limit
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let new_config = self.base_config.clone().with_limit(limit);

        Some(Arc::new(Self {
            base_config: new_config,
            projected_statistics: self.projected_statistics.clone(),
            metrics: self.metrics.clone(),
            predicate: self.predicate.clone(),
            pruning_predicate: self.pruning_predicate.clone(),
            page_pruning_predicate: self.page_pruning_predicate.clone(),
            metadata_size_hint: self.metadata_size_hint,
            parquet_file_reader_factory: self.parquet_file_reader_factory.clone(),
            cache: self.cache.clone(),
            table_parquet_options: self.table_parquet_options.clone(),
            schema_adapter_factory: self.schema_adapter_factory.clone(),
        }))
    }
}

fn should_enable_page_index(
    enable_page_index: bool,
    page_pruning_predicate: &Option<Arc<PagePruningAccessPlanFilter>>,
) -> bool {
    enable_page_index
        && page_pruning_predicate.is_some()
        && page_pruning_predicate
            .as_ref()
            .map(|p| p.filter_number() > 0)
            .unwrap_or(false)
}


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

//! A generic stream over file format readers that can be used by
//! any file format that read its files from start to end.
//!
//! Note: Most traits here need to be marked `Sync + Send` to be
//! compliant with the `SendableRecordBatchStream` trait.

use std::collections::VecDeque;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::datasource::physical_plan::file_scan_config::PartitionColumnProjector;
use datafusion::datasource::physical_plan::{FileMeta, FileScanConfig};

use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, Time,
};
use datafusion::physical_plan::RecordBatchStream;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion_common::instant::Instant;
use datafusion_common::ScalarValue;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{ready, FutureExt, Stream, StreamExt};

/// A fallible future that resolves to a stream of [`RecordBatch`]
pub type FileOpenFuture =
    BoxFuture<'static, Result<BoxStream<'static, Result<RecordBatch, ArrowError>>>>;

/// Describes the behavior of the `FileStream` if file opening or scanning fails
pub enum OnError {
    /// Fail the entire stream and return the underlying error
    Fail,
    /// Continue scanning, ignoring the failed file
    Skip,
}

impl Default for OnError {
    fn default() -> Self {
        Self::Fail
    }
}

/// Generic API for opening a file using an [`ObjectStore`] and resolving to a
/// stream of [`RecordBatch`]
///
/// [`ObjectStore`]: object_store::ObjectStore
pub trait FileOpener: Unpin {
    /// Asynchronously open the specified file and return a stream
    /// of [`RecordBatch`]
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture>;
}

/// A stream that iterates record batch by record batch, file over file.
pub struct FileStream<F: FileOpener> {
    /// An iterator over input files.
    file_iter: VecDeque<PartitionedFile>,
    /// The stream schema (file schema including partition columns and after
    /// projection).
    projected_schema: SchemaRef,
    /// The remaining number of records to parse, None if no limit
    remain: Option<usize>,
    /// A generic [`FileOpener`]. Calling `open()` returns a [`FileOpenFuture`],
    /// which can be resolved to a stream of `RecordBatch`.
    file_opener: F,
    /// The partition column projector
    pc_projector: PartitionColumnProjector,
    /// The stream state
    state: FileStreamState,
    /// File stream specific metrics
    file_stream_metrics: FileStreamMetrics,
    /// runtime baseline metrics
    baseline_metrics: BaselineMetrics,
    /// Describes the behavior of the `FileStream` if file opening or scanning fails
    on_error: OnError,
}

/// Represents the state of the next `FileOpenFuture`. Since we need to poll
/// this future while scanning the current file, we need to store the result if it
/// is ready
enum NextOpen {
    Pending(FileOpenFuture),
    Ready(Result<BoxStream<'static, Result<RecordBatch, ArrowError>>>),
}

enum FileStreamState {
    /// The idle state, no file is currently being read
    Idle,
    /// Currently performing asynchronous IO to obtain a stream of RecordBatch
    /// for a given file
    Open {
        /// A [`FileOpenFuture`] returned by [`FileOpener::open`]
        future: FileOpenFuture,
        /// The partition values for this file
        partition_values: Vec<ScalarValue>,
    },
    /// Scanning the [`BoxStream`] returned by the completion of a [`FileOpenFuture`]
    /// returned by [`FileOpener::open`]
    Scan {
        /// Partitioning column values for the current batch_iter
        partition_values: Vec<ScalarValue>,
        /// The reader instance
        reader: BoxStream<'static, Result<RecordBatch, ArrowError>>,
        /// A [`FileOpenFuture`] for the next file to be processed,
        /// and its corresponding partition column values, if any.
        /// This allows the next file to be opened in parallel while the
        /// current file is read.
        next: Option<(NextOpen, Vec<ScalarValue>)>,
    },
    /// Encountered an error
    Error,
    /// Reached the row limit
    Limit,
}

/// A timer that can be started and stopped.
pub struct StartableTime {
    pub(crate) metrics: Time,
    // use for record each part cost time, will eventually add into 'metrics'.
    pub(crate) start: Option<Instant>,
}

impl StartableTime {
    pub(crate) fn start(&mut self) {
        assert!(self.start.is_none());
        self.start = Some(Instant::now());
    }

    pub(crate) fn stop(&mut self) {
        if let Some(start) = self.start.take() {
            self.metrics.add_elapsed(start);
        }
    }
}

/// Metrics for [`FileStream`]
///
/// Note that all of these metrics are in terms of wall clock time
/// (not cpu time) so they include time spent waiting on I/O as well
/// as other operators.
struct FileStreamMetrics {
    /// Wall clock time elapsed for file opening.
    ///
    /// Time between when [`FileOpener::open`] is called and when the
    /// [`FileStream`] receives a stream for reading.
    ///
    /// If there are multiple files being scanned, the stream
    /// will open the next file in the background while scanning the
    /// current file. This metric will only capture time spent opening
    /// while not also scanning.
    pub time_opening: StartableTime,
    /// Wall clock time elapsed for file scanning + first record batch of decompression + decoding
    ///
    /// Time between when the [`FileStream`] requests data from the
    /// stream and when the first [`RecordBatch`] is produced.
    pub time_scanning_until_data: StartableTime,
    /// Total elapsed wall clock time for for scanning + record batch decompression / decoding
    ///
    /// Sum of time between when the [`FileStream`] requests data from
    /// the stream and when a [`RecordBatch`] is produced for all
    /// record batches in the stream. Note that this metric also
    /// includes the time of the parent operator's execution.
    pub time_scanning_total: StartableTime,
    /// Wall clock time elapsed for data decompression + decoding
    ///
    /// Time spent waiting for the FileStream's input.
    pub time_processing: StartableTime,
    /// Count of errors opening file.
    ///
    /// If using `OnError::Skip` this will provide a count of the number of files
    /// which were skipped and will not be included in the scan results.
    pub file_open_errors: Count,
    /// Count of errors scanning file
    ///
    /// If using `OnError::Skip` this will provide a count of the number of files
    /// which were skipped and will not be included in the scan results.
    pub file_scan_errors: Count,
}

impl FileStreamMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let time_opening = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_opening", partition),
            start: None,
        };

        let time_scanning_until_data = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_scanning_until_data", partition),
            start: None,
        };

        let time_scanning_total = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_scanning_total", partition),
            start: None,
        };

        let time_processing = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_processing", partition),
            start: None,
        };

        let file_open_errors =
            MetricBuilder::new(metrics).counter("file_open_errors", partition);

        let file_scan_errors =
            MetricBuilder::new(metrics).counter("file_scan_errors", partition);

        Self {
            time_opening,
            time_scanning_until_data,
            time_scanning_total,
            time_processing,
            file_open_errors,
            file_scan_errors,
        }
    }
}

impl<F: FileOpener> FileStream<F> {
    /// Create a new `FileStream` using the give `FileOpener` to scan underlying files
    pub fn new(
    object_store_url: ObjectStoreUrl,
    file_schema: SchemaRef,
    file_groups: Vec<Vec<PartitionedFile>>,
    statistics: Statistics,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    table_partition_cols: Vec<Field>,
    output_ordering: Vec<LexOrdering>,

        partition: usize,
        file_opener: F,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        let (projected_schema, ..) = config.project();
        let pc_projector = PartitionColumnProjector::new(
            Arc::clone(&projected_schema),
            &config
                .table_partition_cols
                .iter()
                .map(|x| x.name().clone())
                .collect::<Vec<_>>(),
        );

        let files = config.file_groups[partition].clone();

        Ok(Self {
            file_iter: files.into(),
            projected_schema,
            remain: config.limit,
            file_opener,
            pc_projector,
            state: FileStreamState::Idle,
            file_stream_metrics: FileStreamMetrics::new(metrics, partition),
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            on_error: OnError::Fail,
        })
    }

    /// Specify the behavior when an error occurs opening or scanning a file
    ///
    /// If `OnError::Skip` the stream will skip files which encounter an error and continue
    /// If `OnError:Fail` (default) the stream will fail and stop processing when an error occurs
    pub fn with_on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Begin opening the next file in parallel while decoding the current file in FileStream.
    ///
    /// Since file opening is mostly IO (and may involve a
    /// bunch of sequential IO), it can be parallelized with decoding.
    fn start_next_file(&mut self) -> Option<Result<(FileOpenFuture, Vec<ScalarValue>)>> {
        let part_file = self.file_iter.pop_front()?;

        let file_meta = FileMeta {
            object_meta: part_file.object_meta,
            range: part_file.range,
            extensions: part_file.extensions,
            metadata_size_hint: part_file.metadata_size_hint,
        };

        Some(
            self.file_opener
                .open(file_meta)
                .map(|future| (future, part_file.partition_values)),
        )
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                FileStreamState::Idle => {
                    self.file_stream_metrics.time_opening.start();

                    match self.start_next_file().transpose() {
                        Ok(Some((future, partition_values))) => {
                            self.state = FileStreamState::Open {
                                future,
                                partition_values,
                            }
                        }
                        Ok(None) => return Poll::Ready(None),
                        Err(e) => {
                            self.state = FileStreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                FileStreamState::Open {
                    future,
                    partition_values,
                } => match ready!(future.poll_unpin(cx)) {
                    Ok(reader) => {
                        let partition_values = mem::take(partition_values);

                        // include time needed to start opening in `start_next_file`
                        self.file_stream_metrics.time_opening.stop();
                        let next = self.start_next_file().transpose();
                        self.file_stream_metrics.time_scanning_until_data.start();
                        self.file_stream_metrics.time_scanning_total.start();

                        match next {
                            Ok(Some((next_future, next_partition_values))) => {
                                self.state = FileStreamState::Scan {
                                    partition_values,
                                    reader,
                                    next: Some((
                                        NextOpen::Pending(next_future),
                                        next_partition_values,
                                    )),
                                };
                            }
                            Ok(None) => {
                                self.state = FileStreamState::Scan {
                                    reader,
                                    partition_values,
                                    next: None,
                                };
                            }
                            Err(e) => {
                                self.state = FileStreamState::Error;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                    Err(e) => {
                        self.file_stream_metrics.file_open_errors.add(1);
                        match self.on_error {
                            OnError::Skip => {
                                self.file_stream_metrics.time_opening.stop();
                                self.state = FileStreamState::Idle
                            }
                            OnError::Fail => {
                                self.state = FileStreamState::Error;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                },
                FileStreamState::Scan {
                    reader,
                    partition_values,
                    next,
                } => {
                    // We need to poll the next `FileOpenFuture` here to drive it forward
                    if let Some((next_open_future, _)) = next {
                        if let NextOpen::Pending(f) = next_open_future {
                            if let Poll::Ready(reader) = f.as_mut().poll(cx) {
                                *next_open_future = NextOpen::Ready(reader);
                            }
                        }
                    }
                    match ready!(reader.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();
                            let result = self
                                .pc_projector
                                .project(batch, partition_values)
                                .map_err(|e| ArrowError::ExternalError(e.into()))
                                .map(|batch| match &mut self.remain {
                                    Some(remain) => {
                                        if *remain > batch.num_rows() {
                                            *remain -= batch.num_rows();
                                            batch
                                        } else {
                                            let batch = batch.slice(0, *remain);
                                            self.state = FileStreamState::Limit;
                                            *remain = 0;
                                            batch
                                        }
                                    }
                                    None => batch,
                                });

                            if result.is_err() {
                                // If the partition value projection fails, this is not governed by
                                // the `OnError` behavior
                                self.state = FileStreamState::Error
                            }
                            self.file_stream_metrics.time_scanning_total.start();
                            return Poll::Ready(Some(result.map_err(Into::into)));
                        }
                        Some(Err(err)) => {
                            self.file_stream_metrics.file_scan_errors.add(1);
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();

                            match self.on_error {
                                // If `OnError::Skip` we skip the file as soon as we hit the first error
                                OnError::Skip => match mem::take(next) {
                                    Some((future, partition_values)) => {
                                        self.file_stream_metrics.time_opening.start();

                                        match future {
                                            NextOpen::Pending(future) => {
                                                self.state = FileStreamState::Open {
                                                    future,
                                                    partition_values,
                                                }
                                            }
                                            NextOpen::Ready(reader) => {
                                                self.state = FileStreamState::Open {
                                                    future: Box::pin(std::future::ready(
                                                        reader,
                                                    )),
                                                    partition_values,
                                                }
                                            }
                                        }
                                    }
                                    None => return Poll::Ready(None),
                                },
                                OnError::Fail => {
                                    self.state = FileStreamState::Error;
                                    return Poll::Ready(Some(Err(err.into())));
                                }
                            }
                        }
                        None => {
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();

                            match mem::take(next) {
                                Some((future, partition_values)) => {
                                    self.file_stream_metrics.time_opening.start();

                                    match future {
                                        NextOpen::Pending(future) => {
                                            self.state = FileStreamState::Open {
                                                future,
                                                partition_values,
                                            }
                                        }
                                        NextOpen::Ready(reader) => {
                                            self.state = FileStreamState::Open {
                                                future: Box::pin(std::future::ready(
                                                    reader,
                                                )),
                                                partition_values,
                                            }
                                        }
                                    }
                                }
                                None => return Poll::Ready(None),
                            }
                        }
                    }
                }
                FileStreamState::Error | FileStreamState::Limit => {
                    return Poll::Ready(None)
                }
            }
        }
    }
}

impl<F: FileOpener> Stream for FileStream<F> {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.file_stream_metrics.time_processing.start();
        let result = self.poll_inner(cx);
        self.file_stream_metrics.time_processing.stop();
        self.baseline_metrics.record_poll(result)
    }
}

impl<F: FileOpener> RecordBatchStream for FileStream<F> {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }
}
