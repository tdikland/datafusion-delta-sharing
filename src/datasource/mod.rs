//! Datafusion TableProvider for Delta Sharing
//!
//! Example:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # use datafusion_delta_sharing::error::DeltaSharingError;
//! # async {
//! use std::sync::Arc;
//! use datafusion::prelude::*;
//!
//! use datafusion_delta_sharing::DeltaSharingTable;
//!
//! let ctx = SessionContext::new();
//! let table = DeltaSharingTable::try_from_str("./path/to/profile.share#share.schema.table").await?;
//!
//! ctx.register_table("demo", Arc::new(table))?;
//! ctx.sql("select * from demo").await?.show().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(()) };
//! # Ok(()) }
//! ```

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
    error::DeltaSharingError,
    profile::Profile,
    securable::Table,
};

use self::{scan::DeltaSharingScanBuilder, schema::StructType};

mod expr;
mod reader;
mod scan;
mod schema;

#[derive(Debug, Default)]
pub struct DeltaSharingTableBuilder {
    profile: Option<Profile>,
    table: Option<Table>,
}

impl DeltaSharingTableBuilder {
    pub fn new(profile: Profile, table: Table) -> Self {
        Self {
            profile: Some(profile),
            table: Some(table),
        }
    }

    pub fn with_profile(mut self, profile: Profile) -> Self {
        self.profile = Some(profile);
        self
    }

    pub fn with_table(mut self, table: Table) -> Self {
        self.table = Some(table);
        self
    }

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

pub struct DeltaSharingTable {
    client: DeltaSharingClient,
    table: Table,
    _protocol: Protocol,
    metadata: Metadata,
}

impl DeltaSharingTable {
    pub async fn try_from_str(s: &str) -> Result<Self, DeltaSharingError> {
        let (profile_path, table_fqn) = s.split_once('#').ok_or(DeltaSharingError::other("The connection string should be formatted as `<path/to/profile>#<share_name>.<schema_name>.<table_name>"))?;
        let profile = Profile::try_from_path(profile_path)?;
        let table = table_fqn.parse::<Table>()?;

        DeltaSharingTableBuilder::new(profile, table).build().await
    }

    fn arrow_schema(&self) -> SchemaRef {
        let s: StructType = serde_json::from_str(&self.metadata.schema_string()).unwrap();
        let fields = s
            .fields()
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<Field>, _>>()
            .unwrap();
        Arc::new(Schema::new(fields))
    }

    fn schema(&self) -> Schema {
        let s: StructType = serde_json::from_str(&self.metadata.schema_string()).unwrap();
        let fields = s
            .fields()
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<Field>, _>>()
            .unwrap();
        Schema::new(fields)
    }

    pub fn client(&self) -> &DeltaSharingClient {
        &self.client
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    async fn list_files_for_scan(&self, _filters: String, limit: Option<usize>) -> Vec<File> {
        let mapped_limit = limit.map(|l| l as u32);
        self.client
            .get_table_data(&self.table, None, mapped_limit)
            .await
            .unwrap()
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
        // Fetch files satisfying filters & limit (best effort)
        let combined_filters = conjunction(filters.iter().cloned()).unwrap();
        let filters = expr::Op::from_expr(&combined_filters, self.arrow_schema())?;

        let files = self.list_files_for_scan(filters.to_string(), limit).await;
        let scan = DeltaSharingScanBuilder::new(self.schema(), self.partition_columns())
            .with_projection(projection.cloned())
            .with_files(files)
            .build()
            .unwrap();

        Ok(Arc::new(scan))
    }

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> DataFusionResult<TableProviderFilterPushDown> {
        let op = expr::Op::from_expr(filter, self.arrow_schema());
        if op.is_ok() {
            return Ok(TableProviderFilterPushDown::Inexact);
        } else {
            return Ok(TableProviderFilterPushDown::Unsupported);
        }
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}
