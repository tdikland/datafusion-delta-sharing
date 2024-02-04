//! Delta Sharing table

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    common::{stats::Statistics, Constraints},
    datasource::TableProvider,
    error::Result as DataFusionResult,
    execution::context::SessionState,
    logical_expr::{utils::conjunction, Expr, LogicalPlan, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};

use crate::{
    client::{
        action::{File, Metadata, Protocol},
        DeltaSharingClient,
    },
    securable::Table,
    DeltaSharingError, Profile,
};

use super::{expr::Op, scan::DeltaSharingScanBuilder, schema::StructType};

/// Builder for [`DeltaSharingTable`]
#[derive(Debug, Default)]
pub struct DeltaSharingTableBuilder {
    profile: Option<Profile>,
    table: Option<Table>,
}

impl DeltaSharingTableBuilder {
    /// Create a new DeltaSharingTableBuilder
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the profile for the DeltaSharingTable
    pub fn with_profile(mut self, profile: Profile) -> Self {
        self.profile = Some(profile);
        self
    }

    /// Set the table for the DeltaSharingTable
    pub fn with_table(mut self, table: Table) -> Self {
        self.table = Some(table);
        self
    }

    /// Build the DeltaSharingTable
    pub async fn build(self) -> Result<DeltaSharingTable, DeltaSharingError> {
        let (Some(profile), Some(table)) = (self.profile, self.table) else {
            return Err(DeltaSharingError::other("Missing profile or table"));
        };

        let client = DeltaSharingClient::new(profile);
        let (protocol, metadata) = client.get_table_metadata(&table).await?;

        Ok(DeltaSharingTable {
            client,
            table,
            _protocol: protocol,
            metadata,
        })
    }
}

/// Delta Sharing implementation of [`TableProvider`]`
pub struct DeltaSharingTable {
    client: DeltaSharingClient,
    table: Table,
    _protocol: Protocol,
    metadata: Metadata,
}

impl DeltaSharingTable {
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
    /// let table = DeltaSharingTable::try_from_str("./path/to/profile.share#share.schema.table").await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(()) };
    /// # Ok(()) }
    /// ```
    pub async fn try_from_str(s: &str) -> Result<Self, DeltaSharingError> {
        let (profile_path, table_fqn) = s.split_once('#').ok_or(DeltaSharingError::other("The connection string should be formatted as `<path/to/profile>#<share_name>.<schema_name>.<table_name>"))?;
        let profile = Profile::try_from_path(profile_path)?;
        let table = table_fqn.parse::<Table>()?;

        DeltaSharingTableBuilder::new()
            .with_profile(profile)
            .with_table(table)
            .build()
            .await
    }

    fn arrow_schema(&self) -> SchemaRef {
        let s: StructType = serde_json::from_str(self.metadata.schema_string()).unwrap();
        let fields = s
            .fields()
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<Field>, _>>()
            .unwrap();
        Arc::new(Schema::new(fields))
    }

    fn schema(&self) -> Schema {
        let s: StructType = serde_json::from_str(self.metadata.schema_string()).unwrap();
        let fields = s
            .fields()
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<Field>, _>>()
            .unwrap();
        Schema::new(fields)
    }

    async fn list_files_for_scan(
        &self,
        filter: Option<Op>,
        limit: Option<usize>,
    ) -> Result<Vec<File>, DeltaSharingError> {
        let mapped_limit = limit.map(|l| l as u32);
        let mapped_filter = filter.map(|f| f.to_string_repr());
        self.client
            .get_table_data(&self.table, mapped_filter, mapped_limit)
            .await
    }

    fn partition_columns(&self) -> Vec<String> {
        self.metadata.partition_columns().to_vec()
    }
}

#[async_trait::async_trait]
impl TableProvider for DeltaSharingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        None
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        None
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Convert filters to Delta Sharing filter
        let filter = conjunction(filters.iter().cloned())
            .and_then(|expr| Op::from_expr(&expr, self.arrow_schema()).ok());

        // Fetch files satisfying filters & limit (best effort)
        let files = self.list_files_for_scan(filter, limit).await?;

        // Build Delta Sharing scan
        let scan = DeltaSharingScanBuilder::new(self.schema(), self.partition_columns())
            .with_projection(projection.cloned())
            .with_files(files)
            .build()
            .unwrap();

        Ok(Arc::new(scan))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|f| {
                let op = Op::from_expr(f, self.arrow_schema());
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
