use std::{any::Any, sync::Arc};

use client::{action::File, securable::Table, DeltaSharingClient};
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
// use object_store::ObjectStore;
use reqwest::Url;
use schema::StructType;

use object_store::{path::Path, ObjectMeta};

use crate::store::SignedHttpStoreBuilder;
pub mod catalog;
pub mod client;
// mod datasource;
// mod exec;
// mod parquet;
mod protocol;
pub mod schema;

mod store;

pub struct DeltaSharingTableBuilder {
    endpoint: String,
    token: String,
    share_name: String,
    schema_name: String,
    table_name: String,
}

impl DeltaSharingTableBuilder {
    pub fn new(table: &Table) -> Self {
        let endpoint = std::env::var("SHARING_ENDPOINT").unwrap();
        let token = std::env::var("SHARING_TOKEN").unwrap();
        Self {
            endpoint,
            token,
            share_name: table.share_name().to_string(),
            schema_name: table.schema_name().to_string(),
            table_name: table.name().to_string(),
        }
    }

    pub async fn build(self) -> DeltaSharingTable {
        let client = DeltaSharingClient::new(self.endpoint, self.token);

        let table = client::securable::Table::new(
            self.share_name,
            self.schema_name,
            self.table_name,
            None,
            None,
        );

        let metadata = client.get_table_metadata(&table).await;

        DeltaSharingTable {
            client,
            table,
            metadata,
        }
    }
}

pub struct DeltaSharingTable {
    client: DeltaSharingClient,
    table: Table,
    metadata: client::action::Metadata,
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

    async fn list_files_for_scan(&self) -> Vec<File> {
        self.client.get_table_files(&self.table).await
    }
}

// struct DeltaSharingTableProvider {
//     endpoint: String,
//     token: String,
// }

// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// struct ProtocolResponse {
//     #[serde(rename = "minReaderVersion")]
//     min_reader_version: u32,
// }

// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// struct MetadataResponse {
//     id: Option<String>,
//     name: Option<String>,
//     description: Option<String>,
//     schemaString: String,
//     version: Option<u64>,
//     size: Option<u64>,
//     numFiles: Option<u64>,
// }

// impl MetadataResponse {
//     fn to_schema(&self) -> schema::Schema {
//         let s = serde_json::from_str(&self.schemaString).unwrap();
//         s
//     }

//     fn to_arrow_schema(&self) -> SchemaRef {
//         let fields = self
//             .to_schema()
//             .fields()
//             .iter()
//             .map(|f| f.try_into())
//             .collect::<Result<Vec<Field>, _>>()
//             .unwrap();
//         Arc::new(Schema::new(fields))
//     }
// }

// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// enum ParquetResponse {
//     protocol(ProtocolResponse),
//     metaData(MetadataResponse),
// }

// impl DeltaSharingTableProvider {
//     fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Self {
//         Self {
//             endpoint: endpoint.into(),
//             token: token.into(),
//         }
//     }

//     async fn list_shares(&self) {
//         let mut headers = header::HeaderMap::new();
//         headers.insert(
//             header::AUTHORIZATION,
//             header::HeaderValue::from_str(&format!("Bearer {}", self.token)).unwrap(),
//         );

//         let client = reqwest::Client::builder()
//             .default_headers(headers)
//             .build()
//             .unwrap();
//         let res = client
//             .get("https://oregon.cloud.databricks.com/api/2.0/delta-sharing/metastores/b169b504-4c54-49f2-bc3a-adf4b128f36d/shares")
//             .send()
//             .await
//             .unwrap();

//         eprintln!("RESPONSE: {res:?}");

//         let raw = res.text().await.unwrap();
//         let lines = raw
//             .lines()
//             .map(|l| {
//                 println!("FOUND LINE: {l}");
//                 l
//             })
//             .collect::<Vec<_>>();
//         eprintln!("TEXT: {lines:?}");
//     }

//     async fn get_table_metadata(&self) {
//         let mut headers = header::HeaderMap::new();
//         headers.insert(
//             header::AUTHORIZATION,
//             header::HeaderValue::from_str(&format!("Bearer {}", self.token)).unwrap(),
//         );

//         let share = "tim_dikland_share";
//         let schema = "sse";
//         let table = "customers";
//         let url = format!(
//             "{}/shares/{}/schemas/{}/tables/{}/metadata",
//             self.endpoint, share, schema, table
//         );

//         let client = reqwest::Client::builder()
//             .default_headers(headers)
//             .build()
//             .unwrap();
//         let res = client.get(url).send().await.unwrap();

//         eprintln!("RESPONSE: {res:?}");

//         let raw = res.text().await.unwrap();
//         let lines = raw
//             .lines()
//             .map(|l| {
//                 println!("FOUND LINE: {l}");
//                 l
//             })
//             .collect::<Vec<_>>();
//         eprintln!("TEXT: {lines:?}");

//         for line in lines {
//             let d: ParquetResponse = serde_json::from_str(line).unwrap();
//             println!("PARQUET RESPONSE: {d:?}");

//             match d {
//                 ParquetResponse::protocol(_) => (),
//                 ParquetResponse::metaData(m) => {
//                     let s = m.to_schema();
//                     println!("SCHEMA: {s:?}");

//                     let a_s = m.to_arrow_schema();
//                     println!("ARROW SCHEMA: {a_s}");
//                 }
//             }
//         }
//     }

// fn table_metadata(&self) -> MetadataResponse {}

// fn schema(&self) -> SchemaRef {
//     let mut headers = header::HeaderMap::new();
//     headers.insert(
//         header::AUTHORIZATION,
//         header::HeaderValue::from_str(&format!("Bearer {}", self.token)).unwrap(),
//     );

//     let share = "tim_dikland_share";
//     let schema = "sse";
//     let table = "customers";
//     let url = format!(
//         "{}/shares/{}/schemas/{}/tables/{}/metadata",
//         self.endpoint, share, schema, table
//     );

//     let client = reqwest::Client::builder()
//         .default_headers(headers)
//         .build()
//         .unwrap();
//     let res = client.get(url).send().await.unwrap();

//     eprintln!("RESPONSE: {res:?}");

//     let raw = res.text().await.unwrap();
//     let lines = raw
//         .lines()
//         .map(|l| {
//             println!("FOUND LINE: {l}");
//             l
//         })
//         .collect::<Vec<_>>();
//     eprintln!("TEXT: {lines:?}");
// }
// }

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
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let files = self.list_files_for_scan().await;

        let r = &state.runtime_env().object_store_registry;

        let object_store_url = if let Some(file) = files.first() {
            let mut url = Url::parse(file.url()).unwrap();
            url.set_path("/");
            url.set_query(None);
            let osu = ObjectStoreUrl::parse(url);
            osu.unwrap()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        use object_store::ObjectStore;
        let http_store: Arc<dyn ObjectStore + 'static> = Arc::new(
            SignedHttpStoreBuilder::new(String::from(
                "https://databricks-e2demofieldengwest.s3.us-west-2.amazonaws.com",
            ))
            .with_signed_urls(
                files
                    .iter()
                    .map(|f| f.url().to_string())
                    .collect::<Vec<_>>(),
            )
            .build(),
        );
        state
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), http_store);
        use chrono::TimeZone;
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
                    extensions: None,
                };
                pf
            })
            .collect::<Vec<_>>();

        // create the execution plan
        ParquetFormat::default()
            .create_physical_plan(
                state,
                FileScanConfig {
                    object_store_url,
                    file_schema: self.schema(),
                    file_groups: vec![partitioned_files],
                    statistics: Statistics::new_unknown(&self.schema()),
                    projection: projection.cloned(),
                    limit,
                    output_ordering: vec![],
                    table_partition_cols: vec![],
                    infinite_source: false,
                },
                None,
            )
            .await
    }
}

#[derive(Debug)]
struct DeltaSharingScan {
    parquet_scan: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for DeltaSharingScan {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for DeltaSharingScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        todo!()
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // #[tokio::test]
    // async fn it_works() {
    //     let endpoint = std::env::var("SHARING_ENDPOINT").unwrap();
    //     let token = std::env::var("SHARING_TOKEN").unwrap();
    //     let connector = DeltaSharingTableProvider::new(endpoint, token);

    //     let shares = connector.get_table_metadata().await;

    //     assert!(false);
    // }

    #[tokio::test]
    async fn it_works_df() {
        use datafusion::assert_batches_sorted_eq;
        use datafusion::prelude::*;

        let table = Table::new("tim_dikland_share", "sse", "customers", None, None);

        let ctx = SessionContext::new();
        let table = DeltaSharingTableBuilder::new(&table).build().await;

        ctx.register_table("demo", Arc::new(table)).unwrap();

        let df = ctx.sql("select * from demo").await.unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+----+------------+-------+",
            "| ID | moDified   | vaLue |",
            "+----+------------+-------+",
            "| A  | 2021-02-01 | 1     |",
            "| B  | 2021-02-01 | 10    |",
            "| C  | 2021-02-02 | 20    |",
            "| D  | 2021-02-02 | 100   |",
            "+----+------------+-------+",
        ];

        assert_batches_sorted_eq!(&expected, &actual);
    }
}
