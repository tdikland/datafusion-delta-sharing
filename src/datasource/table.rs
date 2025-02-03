//! Delta Sharing implementation of [`TableProvider`]

use std::{any::Any, borrow::Cow, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::{stats::Statistics, Constraints},
    datasource::TableProvider,
    error::Result as DataFusionResult,
    logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};

use crate::{
    client::profile::Profile,
    client::{Client, QueryTableDataOpts, TableMetadata, TableName},
    error::DeltaSharingError,
    model::action::parquet::File,
};

use crate::client::expr::Op;

use super::{
    error::DataSourceError, scan::format::parquet::SharedParquetExec, schema::LogicalTableSchema,
};

/// Delta Sharing implementation of [`TableProvider`]`
#[derive(Debug)]
pub struct DeltaSharingTable {
    client: Client,
    table: TableName,
    table_metadata: TableMetadata,
    schema: LogicalTableSchema,
}

impl DeltaSharingTable {
    /// Create a new DeltaSharingTable
    pub async fn new(profile: Profile, table: TableName) -> Result<Self, DataSourceError> {
        let client = Client::new(profile);
        let table_metadata = client
            .query_table_metadata(table.clone())
            .await
            .map_err(|e| {
                DataSourceError::Client(format!(
                    "Failed to query table metadata for table '{}': {}",
                    table, e
                ))
            })?;
        let table_schema = table_metadata.schema_string().parse().map_err(|e| {
            DataSourceError::ParseTableSchema(format!(
                "Failed to parse table schema for table '{}': {}",
                table, e
            ))
        })?;

        Ok(Self {
            client,
            table,
            table_metadata,
            schema: table_schema,
        })
    }

    /// Create a new DeltaSharingTable from a connection string
    ///
    /// The connection string should be formatted as
    /// `<path/to/profile>#<share_name>.<schema_name>.<table_name>`
    ///
    /// Example:
    /// ```no_run,rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # async {
    /// use datafusion_delta_sharing::DeltaSharingTable;
    ///
    /// let table =
    ///     DeltaSharingTable::try_from_str("./path/to/profile.share#share.schema.table").await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(()) };
    /// # Ok(()) }
    /// ```
    pub async fn try_from_str(s: &str) -> Result<Self, DeltaSharingError> {
        let (profile_path, table_fqn) = s.split_once('#').ok_or(DeltaSharingError::other("The connection string should be formatted as `<path/to/profile>#<share_name>.<schema_name>.<table_name>"))?;
        let profile = Profile::try_from_path(profile_path)?;
        let table = table_fqn.try_into()?;
        Self::new(profile, table).await.map_err(Into::into)
    }

    fn table_schema(&self) -> &LogicalTableSchema {
        &self.schema
    }

    async fn list_files_for_scan(
        &self,
        filter: Option<Op>,
        limit: Option<usize>,
    ) -> Result<Vec<File>, DeltaSharingError> {
        let mut opts = QueryTableDataOpts::default();
        if let Some(limit) = limit {
            opts = opts.with_limit(limit as u32);
        }
        if let Some(filter) = filter {
            opts = opts.with_predicate(filter);
        }

        let table_data = self.client.query_table_data(&self.table, opts).await?;

        Ok(table_data.into_parquet_files())
    }

    // fn partition_columns(&self) -> Vec<String> {
    //     self.metadata.partition_columns().to_vec()
    // }
}

#[async_trait::async_trait]
impl TableProvider for DeltaSharingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.as_arrow()
    }

    fn constraints(&self) -> Option<&Constraints> {
        // TODO: can primary key constraints be derived from metadata?
        None
    }

    fn table_type(&self) -> TableType {
        // TODO: view sharing exists in Databricks. Does it even matter for this function?
        // The tables are read-only anyway
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        None
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        // TODO: Delta Lake specification has generated columns. Should that be plugged in here?
        None
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Split Delta/Parquet into delta scan and parquet scan

        // how are filters being passed? Is it possible to isolate filters on partition values?

        // Convert filters to Delta Sharing filter.
        // If a filter expression from Datafusion is not supported, than it is omitted from the
        // conjunction.
        let mut supported_ops = filters
            .iter()
            .filter_map(|filter| Op::try_from_expr(filter, self.schema.as_arrow()).ok())
            .collect::<Vec<_>>();
        let filter = match supported_ops.len() {
            0 => None,
            1 => Some(supported_ops.swap_remove(0)),
            2.. => Some(Op::and(supported_ops)),
        };

        // Fetch files satisfying filters & limit (best effort)
        let mut files = self.list_files_for_scan(filter, limit).await?;

        files.iter_mut().for_each(|file| {
            file.url = format!("{}{}", self.client.profile().endpoint(), &file.url).to_string()
        });

        println!("{:?}", files);

        tracing::warn!(schema = ?self.schema, "SCHEMA");

        let scan_schema = if let Some(proj) = projection {
            self.table_schema().project(proj)
        } else {
            self.table_schema().full_projection()
        };

        let partition_cols = self.table_metadata.partition_columns().to_vec();
        let exec = SharedParquetExec::new(
            self.schema(),
            files,
            partition_cols,
            self.table_schema().clone(),
            scan_schema,
        );
        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // TODO: partition filters are exact. differentiate between the two?
        filters
            .iter()
            .map(|f| {
                let op = Op::try_from_expr(f, self.schema());
                if op.is_ok() {
                    Ok(TableProviderFilterPushDown::Inexact)
                } else {
                    Ok(TableProviderFilterPushDown::Unsupported)
                }
            })
            .collect()
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}
