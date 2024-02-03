//! Datafusion TableProvider for Delta Sharing
//!
//! Example:
//!
//! ```rust
//! use std::sync::Arc;
//! use datafusion::prelude::*;
//!
//! use datafusion_delta_sharing::datasource::DeltaSharingTableBuilder;
//! use datafusion_delta_sharing::securable::Table;
//!
//! let ctx = SessionContext::new();
//!
//! let endpoint = std::env::var("SHARING_ENDPOINT").unwrap();
//! let token = std::env::var("SHARING_TOKEN").unwrap();
//! let profile = DeltaSharingProfile::new(endpoint, token);
//! let table = Table::new("share", "schema", "table", None, None);
//!
//! let table = DeltaSharingTableBuilder::new(profile.clone(), table.clone())
//!     .with_profile(profile)
//!     .with_table(table)
//!     .build()
//!     .await
//!     .unwrap();
//!
//! ctx.register_table("demo", Arc::new(table)).unwrap();
//!
//! let data = ctx.sql("select * from demo").await.unwrap().collect().await.unwrap();
//! ```

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    common::{stats::Statistics, Constraints},
    datasource::TableProvider,
    error::Result as DataFusionResult,
    execution::context::SessionState,
    logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};

use crate::{
    client::{
        action::{File, Metadata, Protocol},
        DeltaSharingClient,
    },
    error::DeltaSharingError,
    profile::DeltaSharingProfile,
    securable::Table,
};

use self::{scan::DeltaSharingScanBuilder, schema::StructType};

mod reader;
mod scan;
mod schema;

#[derive(Debug, Default)]
pub struct DeltaSharingTableBuilder {
    profile: Option<DeltaSharingProfile>,
    table: Option<Table>,
}

impl DeltaSharingTableBuilder {
    pub fn new(profile: DeltaSharingProfile, table: Table) -> Self {
        Self {
            profile: Some(profile),
            table: Some(table),
        }
    }

    pub fn with_profile(mut self, profile: DeltaSharingProfile) -> Self {
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
        let profile = DeltaSharingProfile::from_path(profile_path)?;
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

    async fn list_files_for_scan(&self, _filters: &[Expr], limit: Option<usize>) -> Vec<File> {
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
        let files = self.list_files_for_scan(filters, limit).await;
        let scan = DeltaSharingScanBuilder::new(self.schema(), self.partition_columns())
            .with_projection(projection.cloned())
            .with_files(files)
            .build()
            .unwrap();

        Ok(Arc::new(scan))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DataFusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}
