use datafusion_delta_sharing::{
    auth::Profile,
    model::{Schema, Share, Table},
    sdk::{Client, QueryTableVersionOpts},
};

use futures::TryStreamExt;
use tracing_test::traced_test;

#[traced_test]
#[tokio::test]
async fn list_shares() {
    let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
    let client = Client::new(profile);

    let shares: Vec<Share> = client.list_shares().await.try_collect().await.unwrap();
    let expected = vec![Share::builder().name("delta_sharing").build()];
    assert_eq!(shares, expected);
}

#[traced_test]
#[tokio::test]
async fn get_share() {
    let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
    let client = Client::new(profile);

    let share = client.get_share("delta_sharing").await.unwrap();
    let expected = Some(Share::builder().name("delta_sharing").build());
    assert_eq!(share, expected);

    let share = client.get_share("non_existent").await.unwrap();
    let expected = None;
    assert_eq!(share, expected);
}

#[traced_test]
#[tokio::test]
async fn list_schemas() {
    let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
    let client = Client::new(profile);

    let schemas: Vec<Schema> = client
        .list_schemas("delta_sharing")
        .await
        .try_collect()
        .await
        .unwrap();
    let expected = vec![Schema::new("delta_sharing", "default")];
    assert_eq!(schemas, expected);
}

#[traced_test]
#[tokio::test]
async fn list_tables_in_share() {
    let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
    let client = Client::new(profile);

    let shares: Vec<String> = client
        .list_tables_in_share("delta_sharing")
        .await
        .try_collect::<Vec<Table>>()
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
        .list_tables_in_schema("delta_sharing", "default")
        .await
        .try_collect::<Vec<Table>>()
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
#[ignore = "head request?"]
async fn query_table_version() {
    let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
    let client = Client::new(profile);

    let opts = QueryTableVersionOpts::default();
    let version = client
        .query_table_version("delta_sharing", "default", "owid-covid-data", &opts)
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
        .query_table_metadata("delta_sharing", "default", "owid-covid-data")
        .await;
    assert!(metadata.is_ok());
}
