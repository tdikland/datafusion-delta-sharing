use datafusion_delta_sharing::{
    auth::Profile,
    model::{SchemaInfo, ShareInfo, TableInfo},
    sdk::{Client, QueryTableVersionOpts},
};

use futures::TryStreamExt;
use tracing_test::traced_test;

#[traced_test]
#[tokio::test]
async fn list_shares() {
    let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
    let client = Client::new(profile);

    let shares: Vec<ShareInfo> = client.list_shares().await.try_collect().await.unwrap();
    let expected = vec![ShareInfo::builder().name("delta_sharing").build()];
    assert_eq!(shares, expected);
}

#[traced_test]
#[tokio::test]
async fn get_share() {
    let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
    let client = Client::new(profile);

    let share = client
        .get_share("delta_sharing".try_into().unwrap())
        .await
        .unwrap();
    let expected = Some(ShareInfo::builder().name("delta_sharing").build());
    assert_eq!(share, expected);

    let share = client
        .get_share("non_existent".try_into().unwrap())
        .await
        .unwrap();
    let expected = None;
    assert_eq!(share, expected);
}

#[traced_test]
#[tokio::test]
async fn list_schemas() {
    let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
    let client = Client::new(profile);

    let schemas: Vec<SchemaInfo> = client
        .list_schemas("delta_sharing".try_into().unwrap())
        .await
        .try_collect()
        .await
        .unwrap();
    let expected = vec![SchemaInfo::new("delta_sharing", "default")];
    assert_eq!(schemas, expected);
}

#[traced_test]
#[tokio::test]
async fn list_tables_in_share() {
    let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
    let client = Client::new(profile);

    let shares: Vec<String> = client
        .list_tables_in_share("delta_sharing".try_into().unwrap())
        .await
        .try_collect::<Vec<TableInfo>>()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name().to_owned())
        .collect();
    let expected_tables = [
        "COVID_19_NYT",
        "boston-housing",
        "flight-asa_2008",
        "lending_club",
        "nyctaxi_2019",
        "nyctaxi_2019_part",
        "owid-covid-data",
    ]
    .to_vec();

    assert_eq!(shares, expected_tables);
}

#[traced_test]
#[tokio::test]
async fn list_tables_in_schema() {
    let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
    let client = Client::new(profile);

    let shares: Vec<String> = client
        .list_tables_in_schema("delta_sharing.default".try_into().unwrap())
        .await
        .try_collect::<Vec<TableInfo>>()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name().to_owned())
        .collect();
    let expected_tables = [
        "COVID_19_NYT",
        "boston-housing",
        "flight-asa_2008",
        "lending_club",
        "nyctaxi_2019",
        "nyctaxi_2019_part",
        "owid-covid-data",
    ]
    .to_vec();

    assert_eq!(shares, expected_tables);
}

#[traced_test]
#[tokio::test]
#[ignore = "open sharing server does not impl GET version, only HEAD version"]
async fn query_table_version() {
    let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
    let client = Client::new(profile);

    let opts = QueryTableVersionOpts::default();
    let version = client
        .query_table_version(
            "delta_sharing.default.owid-covid-data".try_into().unwrap(),
            &opts,
        )
        .await
        .unwrap();
    assert_eq!(version.0, 0);
}

#[traced_test]
#[tokio::test]
async fn query_table_metadata() {
    let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
    let client = Client::new(profile);

    let metadata = client
        .query_table_metadata("delta_sharing.default.owid-covid-data".try_into().unwrap())
        .await;
    assert!(metadata.is_ok());
}
