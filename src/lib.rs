use std::{any::Any, sync::Arc};

use client::{
    action::{File, Metadata, Protocol},
    profile::DeltaSharingProfile,
    DeltaSharingClient,
};
use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::Result,
    execution::{context::SessionState, object_store::ObjectStoreUrl},
    logical_expr::{Expr, TableType},
    physical_plan::{empty::EmptyExec, DisplayAs, ExecutionPlan, Statistics},
};
use error::DeltaSharingError;
// use object_store::ObjectStore;
use reqwest::Url;

use datafusion::datasource::object_store;
use datafusion::datasource::physical_plan::parquet::ParquetExec;

// use object_store::{path::Path, ObjectMeta};

// use crate::store::SignedHttpStoreBuilder;
pub mod catalog;
pub mod client;
mod datasource;
pub mod error;
// mod exec;
// mod parquet;
mod protocol;
pub mod securable;
// mod store;

use securable::Table;

// #[derive(Debug, Default)]
// pub struct DeltaSharingTableBuilder {
//     profile: Option<DeltaSharingProfile>,
//     table: Option<Table>,
// }

// impl DeltaSharingTableBuilder {
//     pub fn new(profile: DeltaSharingProfile, table: Table) -> Self {
//         Self::default()
//     }

//     pub fn with_profile(mut self, profile: DeltaSharingProfile) -> Self {
//         self.profile = Some(profile);
//         self
//     }

//     pub fn with_table(mut self, table: Table) -> Self {
//         self.table = Some(table);
//         self
//     }

//     pub async fn build(self) -> Result<DeltaSharingTable, DeltaSharingError> {
//         let (Some(profile), Some(table)) = (self.profile, self.table) else {
//             return Err(DeltaSharingError::other("Missing profile or table"));
//         };

//         let client = DeltaSharingClient::new(profile);
//         let (protocol, metadata) = client.get_table_metadata(&table).await?;

//         Ok(DeltaSharingTable {
//             client,
//             table,
//             protocol,
//             metadata,
//         })
//     }
// }

// pub struct DeltaSharingTable {
//     client: DeltaSharingClient,
//     table: Table,
//     protocol: Protocol,
//     metadata: Metadata,
// }

// impl DeltaSharingTable {
//     fn arrow_schema(&self) -> SchemaRef {
//         let s: StructType = serde_json::from_str(&self.metadata.schema_string()).unwrap();
//         let fields = s
//             .fields()
//             .iter()
//             .map(|f| f.try_into())
//             .collect::<Result<Vec<Field>, _>>()
//             .unwrap();
//         Arc::new(Schema::new(fields))
//     }

//     pub fn client(&self) -> &DeltaSharingClient {
//         &self.client
//     }

//     pub fn table(&self) -> &Table {
//         &self.table
//     }

//     pub fn protocol(&self) -> &Protocol {
//         &self.protocol
//     }

//     pub fn metadata(&self) -> &Metadata {
//         &self.metadata
//     }

//     async fn list_files_for_scan(&self) -> Vec<File> {
//         self.client
//             .get_table_data(&self.table, None, None)
//             .await
//             .unwrap()
//     }
// }

// #[async_trait::async_trait]
// impl TableProvider for DeltaSharingTable {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }

//     fn schema(&self) -> SchemaRef {
//         self.arrow_schema()
//     }

//     fn table_type(&self) -> TableType {
//         TableType::Base
//     }

//     async fn scan(
//         &self,
//         state: &SessionState,
//         projection: Option<&Vec<usize>>,
//         _filters: &[Expr],
//         limit: Option<usize>,
//     ) -> Result<Arc<dyn ExecutionPlan>> {
//         let files = self.list_files_for_scan().await;

//         let _ = &state.runtime_env().object_store_registry;

//         let object_store_url = if let Some(file) = files.first() {
//             let mut url = Url::parse(file.url()).unwrap();
//             url.set_path("/");
//             url.set_query(None);
//             let osu = ObjectStoreUrl::parse(url);
//             osu.unwrap()
//         } else {
//             return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
//         };

//         use object_store::ObjectStore;
//         let http_store: Arc<dyn ObjectStore + 'static> = Arc::new(
//             SignedHttpStoreBuilder::new(String::from(
//                 "https://databricks-e2demofieldengwest.s3.us-west-2.amazonaws.com",
//             ))
//             .with_signed_urls(
//                 files
//                     .iter()
//                     .map(|f| f.url().to_string())
//                     .collect::<Vec<_>>(),
//             )
//             .build(),
//         );
//         state
//             .runtime_env()
//             .register_object_store(object_store_url.as_ref(), http_store);
//         use chrono::TimeZone;
//         let partitioned_files = files
//             .iter()
//             .map(|f| {
//                 let mut url = Url::parse(f.url()).unwrap();
//                 url.set_query(None);
//                 let pf = PartitionedFile {
//                     object_meta: ObjectMeta {
//                         location: Path::from_url_path(url.path()).unwrap(),
//                         last_modified: chrono::Utc.timestamp_nanos(0),
//                         size: f.size() as usize,
//                         e_tag: None,
//                         version: None,
//                     },
//                     partition_values: vec![],
//                     range: None,
//                     extensions: None,
//                 };
//                 pf
//             })
//             .collect::<Vec<_>>();

//         // create the execution plan
//         ParquetFormat::default()
//             .create_physical_plan(
//                 state,
//                 FileScanConfig {
//                     object_store_url,
//                     file_schema: self.schema(),
//                     file_groups: vec![partitioned_files],
//                     statistics: Statistics::new_unknown(&self.schema()),
//                     projection: projection.cloned(),
//                     limit,
//                     output_ordering: vec![],
//                     table_partition_cols: vec![],
//                     infinite_source: false,
//                 },
//                 None,
//             )
//             .await
//     }
// }

// #[derive(Debug)]
// struct DeltaSharingScan {
//     _parquet_scan: Arc<dyn ExecutionPlan>,
// }

// impl DisplayAs for DeltaSharingScan {
//     fn fmt_as(
//         &self,
//         _t: datafusion::physical_plan::DisplayFormatType,
//         _f: &mut std::fmt::Formatter,
//     ) -> std::fmt::Result {
//         todo!()
//     }
// }

// impl ExecutionPlan for DeltaSharingScan {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }

//     fn schema(&self) -> SchemaRef {
//         todo!()
//     }

//     fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
//         todo!()
//     }

//     fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
//         todo!()
//     }

//     fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
//         todo!()
//     }

//     fn with_new_children(
//         self: Arc<Self>,
//         _children: Vec<Arc<dyn ExecutionPlan>>,
//     ) -> Result<Arc<dyn ExecutionPlan>> {
//         todo!()
//     }

//     fn execute(
//         &self,
//         _partition: usize,
//         _context: Arc<datafusion::execution::TaskContext>,
//     ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
//         todo!()
//     }
// }

#[cfg(test)]
mod test {
    use tracing_test::traced_test;

    use crate::datasource::DeltaSharingTableBuilder;

    use super::*;

    // #[tokio::test]
    // async fn it_works() {
    //     let endpoint = std::env::var("SHARING_ENDPOINT").unwrap();
    //     let token = std::env::var("SHARING_TOKEN").unwrap();
    //     let connector = DeltaSharingTableProvider::new(endpoint, token);

    //     let shares = connector.get_table_metadata().await;

    //     assert!(false);
    // }

    #[traced_test]
    #[tokio::test]
    async fn it_works_df() {
        use datafusion::assert_batches_sorted_eq;
        use datafusion::prelude::*;

        let ctx = SessionContext::new();

        let endpoint = std::env::var("SHARING_ENDPOINT").unwrap();
        let token = std::env::var("SHARING_TOKEN").unwrap();
        let profile = DeltaSharingProfile::new(endpoint, token);
        let table = Table::new("tim_dikland_share", "sse", "config", None, None);

        let table = DeltaSharingTableBuilder::new(profile.clone(), table.clone())
            .with_profile(profile)
            .with_table(table)
            .build()
            .await
            .unwrap();

        ctx.register_table("demo", Arc::new(table)).unwrap();

        let df = ctx.sql("select * from demo").await.unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+----+-----+",
            "| id | val |",
            "+----+-----+",
            "| 1  | foo |",
            "| 2  | bar |",
            "+----+-----+",
        ];

        assert_batches_sorted_eq!(&expected, &actual);
    }
}
