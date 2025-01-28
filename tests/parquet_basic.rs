use std::sync::Arc;

use datafusion::assert_batches_eq;
use datafusion::prelude::*;
use datafusion_delta_sharing::{datasource::DeltaSharingTable, Profile};
use httpmock::MockServer;
use tracing_test::traced_test;

const TABLE_VERSION_NUMBER: &str = "0";
const PROTOCOL: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
const METADATA: &str = r#"{"metaData":{"id":"ced0baf6-aa13-4871-af26-91e6e2787052","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"letter\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"number\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"a_float\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1674611426764}}"#;
const FILE_1: &str = r#"{"file":{"url":"file1","id":"1","size":751,"partitionValues":{},"modificationTime":1674611427093,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"number\":1,\"a_float\":1.1},\"maxValues\":{\"number\":1,\"a_float\":1.1},\"nullCount\":{\"number\":0,\"a_float\":0}}"}}"#;
const FILE_2: &str = r#"{"file":{"url":"file2","id":"2","size":751,"partitionValues":{},"modificationTime":1674611427109,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"number\":2,\"a_float\":2.2},\"maxValues\":{\"number\":2,\"a_float\":2.2},\"nullCount\":{\"number\":0,\"a_float\":0}}"}}"#;
const FILE_3: &str = r#"{"file":{"url":"file3","id":"3","size":751,"partitionValues":{},"modificationTime":1674611427117,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"number\":3,\"a_float\":3.3},\"maxValues\":{\"number\":3,\"a_float\":3.3},\"nullCount\":{\"number\":0,\"a_float\":0}}"}}"#;

#[traced_test]
#[tokio::test]
async fn full_scan() {
    let mock_server = MockServer::start();
    let ctx = SessionContext::new();

    // Mock sharing client calls
    mock_server.mock(|when, then| {
        when.method("GET")
            .path("/shares/test_share/schemas/test_schema/tables/test_table/metadata");
        then.status(200)
            .header("content-type", "application/x-ndjson")
            .header("delta-table-version", TABLE_VERSION_NUMBER)
            .body(format!("{}\n{}", PROTOCOL, METADATA));
    });
    mock_server.mock(|when, then| {
        when.method("POST")
            .path("/shares/test_share/schemas/test_schema/tables/test_table/query");
        then.status(200)
            .header("content-type", "application/x-ndjson")
            .header("delta-table-version", TABLE_VERSION_NUMBER)
            .body(format!(
                "{}\n{}\n{}\n{}\n{}",
                PROTOCOL, METADATA, FILE_1, FILE_2, FILE_3
            ));
    });

    // Mock file reads
    mock_server.mock(|when, then| {
        when.method("GET").path("/file1");
        then.status(200)
            .body_from_file("tests/data/parquet_basic/file1.parquet");
    });
    mock_server.mock(|when, then| {
        when.method("GET").path("/file2");
        then.status(200)
            .body_from_file("tests/data/parquet_basic/file2.parquet");
    });
    mock_server.mock(|when, then| {
        when.method("GET").path("/file3");
        then.status(200)
            .body_from_file("tests/data/parquet_basic/file3.parquet");
    });

    // Register table
    let table = DeltaSharingTable::builder()
        .with_profile(Profile::new_bearer_token(
            1,
            mock_server.base_url(),
            "fake_token",
            None,
        ))
        .with_table("test_share.test_schema.test_table".try_into().unwrap())
        .build()
        .await
        .unwrap();
    ctx.register_table("demo", Arc::new(table)).unwrap();

    // Query table
    let data = ctx.sql("select * from demo").await.unwrap();
    let res = data.collect().await.unwrap();

    assert_batches_eq!(
        &[
            "+--------+---------+",
            "| number | a_float |",
            "+--------+---------+",
            "| 1      | 1.1     |",
            "| 2      | 2.2     |",
            "| 2      | 2.2     |",
            "+--------+---------+",
        ],
        &res
    );
}
