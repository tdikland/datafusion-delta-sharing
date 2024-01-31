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

use std::{any::Any, collections::HashMap, sync::Arc};

use arrow_schema::TimeUnit;
use chrono::TimeZone;
use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileScanConfig, ParquetExec},
        TableProvider,
    },
    execution::{context::SessionState, object_store::ObjectStoreUrl},
    logical_expr::{Expr, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    scalar::ScalarValue,
};
use reqwest::Url;
use tracing::info;

use crate::{
    client::{
        action::{File, Metadata, Protocol},
        profile::DeltaSharingProfile,
        DeltaSharingClient,
    },
    error::DeltaSharingError,
    securable::Table,
};

use self::{
    reader::SignedParquetFileReaderFactory, scan::DeltaSharingScanBuilder, schema::StructType,
};

use object_store::{path::Path, ObjectMeta};

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
            protocol,
            metadata,
        })
    }
}

pub struct DeltaSharingTable {
    client: DeltaSharingClient,
    table: Table,
    protocol: Protocol,
    metadata: Metadata,
}

impl DeltaSharingTable {
    pub async fn try_from_str(_s: &str) -> Result<Self, DeltaSharingError> {
        todo!()
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

    pub fn client(&self) -> &DeltaSharingClient {
        &self.client
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn protocol(&self) -> &Protocol {
        &self.protocol
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    async fn list_files_for_scan(&self) -> Vec<File> {
        self.client
            .get_table_data(&self.table, None, None)
            .await
            .unwrap()
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

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // fetch files satisfying filters & limit
        let files = self.list_files_for_scan().await;

        // handle schema
        let parsed_schema =
            serde_json::from_str::<StructType>(self.metadata.schema_string()).unwrap();

        info!("PARSED SCHEMA:\n:{:?}", parsed_schema);

        let schema: Schema = (&parsed_schema).try_into().unwrap();
        info!("ARROW SCHEMA:\n:{}", schema);

        // handle table partitions
        let partition_fields = schema
            .fields()
            .iter()
            .filter(|field| self.metadata.partition_columns().contains(field.name()))
            .collect::<Vec<_>>();
        // let partition_schema = Arc::new(Schema::new(partition_fields));

        let file_fields = schema
            .fields()
            .into_iter()
            .filter(|field| !self.metadata.partition_columns().contains(field.name()))
            .collect::<Vec<_>>();
        // let file_schema = Arc::new(Schema::new(file_fields));

        let scan_builder = DeltaSharingScanBuilder::new()
            .with_files(files.clone())
            .with_projection(projection.cloned())
            .build();

        // let mut file_groups: HashMap<Vec<ScalarValue>
        let a = files.first().unwrap().partition_values();

        let partitioned_files = files
            .iter()
            .map(|f| {
                let mut url = Url::parse(f.url()).expect("valid URL");
                url.set_query(None);
                let pf = PartitionedFile {
                    object_meta: ObjectMeta {
                        location: Path::from_url_path(url.path()).unwrap(),
                        last_modified: chrono::Utc.timestamp_nanos(0),
                        size: f.size() as usize,
                        e_tag: None,
                        version: None,
                    },
                    partition_values: vec![],
                    range: None,
                    extensions: Some(Arc::new(f.url().to_string())),
                };
                pf
            })
            .collect::<Vec<_>>();

        let table_partition_cols: Vec<String> = vec![];

        let file_schema = Arc::new(Schema::new(
            self.schema()
                .fields()
                .iter()
                .filter(|f| !table_partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<_>>(),
        ));

        let config = FileScanConfig {
            // DeltaSharingScan does not use the ObjectStore abstraction to read files
            // so we set the URL to a local filesystem URL as a cheap default.
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_schema,
            file_groups: vec![partitioned_files],
            statistics: datafusion::common::stats::Statistics::new_unknown(&Schema::empty()),
            projection: projection.cloned(),
            limit,
            output_ordering: vec![],
            table_partition_cols: vec![],
        };
        let pred = None;
        let size_hint = None;

        let exec = ParquetExec::new(config, pred, size_hint)
            .with_enable_bloom_filter(false)
            .with_enable_page_index(false)
            .with_parquet_file_reader_factory(Arc::new(SignedParquetFileReaderFactory::new()));

        Ok(Arc::new(exec))
    }
}

fn pv(partition_schema: Schema, partition_values: HashMap<String, String>) -> Vec<ScalarValue> {
    partition_schema
        .fields()
        .iter()
        .map(|f| {
            partition_values
                .get(f.name())
                .map(|v| ScalarValue::Utf8(Some(v.clone())))
                .unwrap_or_else(|| ScalarValue::Utf8(None))
        })
        .collect::<Vec<_>>()
}

fn deserialize_partition_values(
    partition_columns: &[String],
    partition_fields: Schema,
    partition_values: HashMap<String, Option<String>>,
) -> datafusion::error::Result<Vec<ScalarValue>> {
    for pcol in partition_columns {
        let field = partition_fields
            .field_with_name(pcol)
            .map_err(|_| ())
            .unwrap();
        let partition_value: Option<Option<&str>> =
            partition_values.get(field.name()).map(|v| v.as_deref());

        let scalar = match partition_value {
            None | Some(None) | Some(Some("")) => {
                ScalarValue::try_from_string("".to_string(), field.data_type())?
            }
            Some(Some(str_value)) => match &field.data_type() {
                arrow_schema::DataType::Boolean
                | arrow_schema::DataType::Int8
                | arrow_schema::DataType::Int16
                | arrow_schema::DataType::Int32
                | arrow_schema::DataType::Int64
                | arrow_schema::DataType::Float32
                | arrow_schema::DataType::Float64
                | arrow_schema::DataType::Utf8
                | arrow_schema::DataType::Decimal128(_, _)
                | arrow_schema::DataType::Decimal256(_, _) => {
                    ScalarValue::try_from_string(str_value.to_string(), field.data_type())?
                }
                arrow_schema::DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    todo!()
                }
                _ => todo!(),
            },
        };

        // let scalar = match field.data_type() {
        //     arrow_schema::DataType::Boolean => {
        //         ScalarValue::try_from_string(a.to_owned(), field.data_type())
        //     }
        //     arrow_schema::DataType::Int8 => todo!(),
        //     arrow_schema::DataType::Int16 => todo!(),
        //     arrow_schema::DataType::Int32 => todo!(),
        //     arrow_schema::DataType::Int64 => todo!(),
        //     // arrow_schema::DataType::UInt8 => todo!(),
        //     // arrow_schema::DataType::UInt16 => todo!(),
        //     // arrow_schema::DataType::UInt32 => todo!(),
        //     // arrow_schema::DataType::UInt64 => todo!(),
        //     // arrow_schema::DataType::Float16 => todo!(),
        //     arrow_schema::DataType::Float32 => todo!(),
        //     arrow_schema::DataType::Float64 => todo!(),
        //     arrow_schema::DataType::Timestamp(TimeUnit::Microsecond, None) => todo!(),
        //     arrow_schema::DataType::Date32 => todo!(),
        //     // arrow_schema::DataType::Date64 => todo!(),
        //     // arrow_schema::DataType::Time32(_) => todo!(),
        //     // arrow_schema::DataType::Time64(_) => todo!(),
        //     // arrow_schema::DataType::Duration(_) => todo!(),
        //     // arrow_schema::DataType::Interval(_) => todo!(),
        //     // arrow_schema::DataType::Binary => todo!(),
        //     // arrow_schema::DataType::FixedSizeBinary(_) => todo!(),
        //     // arrow_schema::DataType::LargeBinary => todo!(),
        //     arrow_schema::DataType::Utf8 => todo!(),
        //     // arrow_schema::DataType::LargeUtf8 => todo!(),
        //     // arrow_schema::DataType::List(_) => todo!(),
        //     // arrow_schema::DataType::FixedSizeList(_, _) => todo!(),
        //     // arrow_schema::DataType::LargeList(_) => todo!(),
        //     // arrow_schema::DataType::Struct(_) => todo!(),
        //     // arrow_schema::DataType::Union(_, _) => todo!(),
        //     // arrow_schema::DataType::Dictionary(_, _) => todo!(),
        //     arrow_schema::DataType::Decimal128(_, _) => todo!(),
        //     arrow_schema::DataType::Decimal256(_, _) => todo!(),
        //     // arrow_schema::DataType::Map(_, _) => todo!(),
        //     // arrow_schema::DataType::RunEndEncoded(_, _) => todo!(),
        //     _ => return Err(datafusion::error::DataFusionError::Internal(String::new())),
        // };
    }

    todo!()
}
