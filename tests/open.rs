// use datafusion_delta_sharing::{
//     client::DeltaSharingClient,
//     securable::{Schema, Share, Table},
//     Profile,
// };

// use tracing_test::traced_test;

// #[traced_test]
// #[tokio::test]
// async fn list_shares() {
//     let profile = Profile::try_from_path("./tests/open-datasets.share").unwrap();
//     let client = DeltaSharingClient::new(profile);

//     let table: Table = "delta_sharing.default.delta_sharing".parse().unwrap();
//     let shares = client.get_table_data(&table, None, None).await.unwrap();

//     assert!(true)
// }

// #[traced_test]
// #[tokio::test]
// async fn list_schemas() {
//     let profile = DeltaSharingProfile::from_path("./tests/open-datasets.share");
//     let client = DeltaSharingClient::new(profile);

//     let schema = Share::new("delta_sharing", None);
//     let shares = client.list_schemas(&schema).await.unwrap();

//     assert_eq!(shares, vec![Schema::new("delta_sharing", "default")]);
// }

// #[traced_test]
// #[tokio::test]
// async fn list_tables_in_share() {
//     let profile = DeltaSharingProfile::from_path("./tests/open-datasets.share");
//     let client = DeltaSharingClient::new(profile);

//     let share = Share::new("delta_sharing", None);
//     let shares = client.list_all_tables(&share).await.unwrap();

//     let expected_tables = [
//         "COVID_19_NYT",
//         "boston-housing",
//         "flight-asa_2008",
//         "lending_club",
//         "nyctaxi_2019",
//         "nyctaxi_2019_part",
//         "owid-covid-data",
//     ]
//     .into_iter()
//     .map(|t| Table::new("delta_sharing", "default", t, None, None))
//     .collect::<Vec<_>>();

//     assert_eq!(shares, expected_tables);
// }

// #[traced_test]
// #[tokio::test]
// async fn list_tables_in_schema() {
//     let profile = DeltaSharingProfile::from_path("./tests/open-datasets.share");
//     let client = DeltaSharingClient::new(profile);

//     let schema = Schema::new("delta_sharing", "default");
//     let shares = client.list_tables(&schema).await.unwrap();

//     let expected_tables = [
//         "COVID_19_NYT",
//         "boston-housing",
//         "flight-asa_2008",
//         "lending_club",
//         "nyctaxi_2019",
//         "nyctaxi_2019_part",
//         "owid-covid-data",
//     ]
//     .into_iter()
//     .map(|t| Table::new("delta_sharing", "default", t, None, None))
//     .collect::<Vec<_>>();

//     assert_eq!(shares, expected_tables);
// }
