// mod exec;
// mod reader;

use std::{any::Any, sync::Arc};

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
};
use reqwest::Url;

use crate::{
    client::{
        action::{File, Metadata, Protocol},
        profile::DeltaSharingProfile,
        DeltaSharingClient,
    },
    error::DeltaSharingError,
    securable::Table,
};

use self::{reader::SignedParquetFileReaderFactory, schema::StructType};

use object_store::{path::Path, ObjectMeta};

mod reader;
mod schema;

#[derive(Debug, Default)]
pub struct DeltaSharingTableBuilder {
    profile: Option<DeltaSharingProfile>,
    table: Option<Table>,
}

impl DeltaSharingTableBuilder {
    pub fn new(profile: DeltaSharingProfile, table: Table) -> Self {
        Self::default()
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
        let files = self.list_files_for_scan().await;
        let partitioned_files = files
            .iter()
            .map(|f| {
                let mut url = Url::parse(f.url()).unwrap();
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

// async fn p() -> Arc<dyn ExecutionPlan> {
//     let config = FileScanConfig {
//         object_store_url: ObjectStoreUrl::parse("file:///").unwrap(),
//         file_schema: Arc::new(Schema::empty()),
//         file_groups: vec![],
//         statistics: datafusion::common::stats::Statistics::new_unknown(&Schema::empty()),
//         projection: projection,
//         limit: None,
//         output_ordering: vec![],
//         table_partition_cols: vec![],
//     };
//     let pred = None;
//     let size_hint = None;

//     let exec = ParquetExec::new(config, pred, size_hint)
//         .with_enable_bloom_filter(false)
//         .with_enable_page_index(false)
//         .with_parquet_file_reader_factory(Arc::new(SignedParquetFileReaderFactory::new()));

//     Arc::new(exec)
// }
