use std::fs::File;
use std::io::BufRead;
use std::sync::Arc;

use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;
use datafusion_delta_sharing::profile::ProfileType;
use datafusion_delta_sharing::{datasource::DeltaSharingTable, Profile};
use httpmock::MockServer;
use tracing_test::traced_test;

const TABLE_VERSION_NUMBER: &str = "0";

#[traced_test]
#[tokio::test]
async fn full_scan() {
    let mock_server = MockServer::start();
    let ctx = SessionContext::new();

    let actions = File::open("tests/data/parquet_basic/actions.ndjson").unwrap();
    let mut lines = std::io::BufReader::new(actions).lines();

    let protocol = lines.next().unwrap().unwrap();
    let metadata = lines.next().unwrap().unwrap();
    let add_actions = lines.map(|i| i.unwrap()).collect::<Vec<_>>().join("\n");

    // Mock sharing client calls
    mock_server.mock(|when, then| {
        when.method("GET")
            .path("/shares/test_share/schemas/test_schema/tables/test_table/metadata");
        then.status(200)
            .header("content-type", "application/x-ndjson")
            .header("delta-table-version", TABLE_VERSION_NUMBER)
            .body([protocol.clone(), metadata.clone()].join("\n"));
    });
    println!("{}", [protocol.clone(), metadata.clone()].join("\n"));

    mock_server.mock(|when, then| {
        when.method("POST")
            .path("/shares/test_share/schemas/test_schema/tables/test_table/query");
        then.status(200)
            .header("content-type", "application/x-ndjson")
            .header("delta-table-version", TABLE_VERSION_NUMBER)
            .body([protocol, metadata, add_actions].join("\n"));
    });

    // Mock file reads
    mock_server.mock(|when, then| {
        when.method("GET").path("/file1.parquet");
        then.status(200)
            .body(std::fs::read("tests/data/parquet_basic/file1.parquet").unwrap());
    });
    mock_server.mock(|when, then| {
        when.method("GET").path("/file2.parquet");
        then.status(200)
            .body(std::fs::read("tests/data/parquet_basic/file2.parquet").unwrap());
    });

    // Register table
    let table = DeltaSharingTable::new(
        Profile::from_profile_type(
            1,
            mock_server.base_url().parse().unwrap(),
            ProfileType::new_bearer_token("foo", None),
        ),
        "test_share.test_schema.test_table".try_into().unwrap(),
    )
    .await
    .unwrap();
    ctx.register_table("demo", Arc::new(table)).unwrap();

    // Query table
    let data = ctx.sql("select * from demo").await.unwrap();
    let res = data.collect().await.unwrap();

    assert_batches_sorted_eq!(
        &[
            "+----+-------+",
            "| id | value |",
            "+----+-------+",
            "| 1  | hello |",
            "| 2  | world |",
            "| 3  | foo   |",
            "| 4  | bar   |",
            "| 4  | baz   |",
            "+----+-------+",
        ],
        &res
    );
}
