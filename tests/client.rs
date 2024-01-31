// use datafusion_delta_sharing::{
//     client::{
//         action::{MetadataBuilder, Protocol},
//         pagination::Pagination,
//         profile::DeltaSharingProfile,
//         DeltaSharingClient,
//     },
//     securable::Share,
// };
// use httpmock::MockServer;

// #[tokio::test]
// async fn list_shares_paginated() {
//     let server = MockServer::start();
//     let mock = server.mock(|when, then| {
//         when.method("GET")
//             .path("/shares")
//             .query_param("maxResults", "1")
//             .query_param("pageToken", "foo")
//             .header_exists("authorization");
//         then.status(200)
//             .header("content-type", "application/json")
//             .header("charset", "utf-8")
//             .body_from_file("tests/api_responses/list_shares_paginated.txt");
//     });

//     let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
//     let client = DeltaSharingClient::new(profile);
//     let result = client
//         .list_shares_paginated(&Pagination::start(Some(1), Some("foo".into())))
//         .await
//         .unwrap();

//     mock.assert();
//     assert_eq!(
//         result.items(),
//         vec![Share::new("test_share", Some("test_id"))]
//     );
//     assert_eq!(result.next_page_token(), Some("test_token"));
// }

// #[tokio::test]
// async fn list_shares() {
//     let server = MockServer::start();
//     let mock = server.mock(|when, then| {
//         when.method("GET")
//             .path("/shares")
//             .header_exists("authorization");
//         then.status(200)
//             .header("content-type", "application/json")
//             .header("charset", "utf-8")
//             .body_from_file("tests/api_responses/list_shares.txt");
//     });

//     let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
//     let client = DeltaSharingClient::new(profile);
//     let result = client.list_shares().await.unwrap();

//     mock.assert();
//     assert_eq!(result, vec![Share::new("test_share", Some("test_id"))]);
// }

// #[tokio::test]
// async fn get_table_metadata() {
//     let server = MockServer::start();
//     let mock = server.mock(|when, then| {
//         when.method("GET")
//             .path("/shares/foo/schemas/bar/tables/baz/metadata")
//             .header_exists("authorization");
//         then.status(200)
//             .header("content-type", "application/json")
//             .header("charset", "utf-8")
//             .body_from_file("tests/api_responses/get_table_metadata.txt");
//     });

//     let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
//     let client = DeltaSharingClient::new(profile);
//     let table = "foo.bar.baz".parse().unwrap();
//     let (protocol, metadata) = client.get_table_metadata(&table).await.unwrap();

//     let schema_string = "{\"type\":\"struct\",\"fields\":[{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}";
//     let expected_metadata = MetadataBuilder::new("m_id", schema_string)
//         .partition_columns(vec!["date".to_string()])
//         .build();

//     mock.assert();
//     assert_eq!(protocol, Protocol::default());
//     assert_eq!(metadata, expected_metadata);
// }

// // #[cfg(test)]
// // mod test {
// //     use super::*;
// //     use httpmock::prelude::*;
// //     use serde_json::json;

// //     #[tokio::test]
// //     async fn unauthenticated() {
// //         let server = MockServer::start();
// //         let mock = server.mock(|when, then| {
// //             when.method(GET)
// //                 .path("/shares")
// //                 .query_param("maxResults", "1")
// //                 .query_param("pageToken", "foo")
// //                 .header_exists("authorization");
// //             then.status(401)
// //                 .header("content-type", "application/json")
// //                 .header("charset", "utf-8")
// //                 .json_body(json!(
// //                     {
// //                         "errorCode": "UNAUTHENTICATED",
// //                         "message": "The request was not authenticated"
// //                     }
// //                 ));
// //         });

// //         let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
// //         let client = DeltaSharingClient::new(profile);
// //         let result = client
// //             .list_shares_paginated(&Pagination::start(Some(1), Some("foo".into())))
// //             .await;

// //         mock.assert();
// //         assert!(result.is_err());
// //     }

// //     #[tokio::test]
// //     async fn list_all_tables_paginated() {
// //         let server = MockServer::start();
// //         let mock = server.mock(|when, then| {
// //             when.method(GET)
// //                 .path("/shares/foo/all-tables")
// //                 .query_param("maxResults", "1")
// //                 .query_param("pageToken", "bar")
// //                 .header_exists("authorization");
// //             then.status(200)
// //                 .header("content-type", "application/json")
// //                 .header("charset", "utf-8")
// //                 .json_body(json!(
// //                     {
// //                         "items": [
// //                             {
// //                                 "name": "baz",
// //                                 "schema": "qux",
// //                                 "share": "quux",
// //                                 "shareId": "corge",
// //                                 "id": "grault"
// //                             }
// //                         ],
// //                         "nextPageToken": "garply"
// //                     }
// //                 ));
// //         });

// //         let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
// //         let client = DeltaSharingClient::new(profile);
// //         let result = client
// //             .list_all_tables_paginated("foo", &Pagination::start(Some(1), Some("bar".into())))
// //             .await
// //             .unwrap();

// //         mock.assert();
// //         assert_eq!(result.items.len(), 1);
// //         assert_eq!(result.items[0].name(), "baz");
// //         assert_eq!(result.items[0].schema_name(), "qux");
// //         assert_eq!(result.items[0].share_name(), "quux");
// //         assert_eq!(result.items[0].share_id(), Some("corge".into()));
// //         assert_eq!(result.items[0].id(), Some("grault".into()));
// //         assert_eq!(result.next_page_token, Some("garply".into()));
// //     }

// //     #[tokio::test]
// //     async fn list_all_tables() {
// //         let server = MockServer::start();
// //         let mock = server.mock(|when, then| {
// //             when.method(GET)
// //                 .path("/shares/foo/all-tables")
// //                 .header_exists("authorization");
// //             then.status(200)
// //                 .header("content-type", "application/json")
// //                 .header("charset", "utf-8")
// //                 .json_body(json!(
// //                     {
// //                         "items": [
// //                             {
// //                                 "name": "baz",
// //                                 "schema": "qux",
// //                                 "share": "quux",
// //                                 "shareId": "corge",
// //                                 "id": "grault"
// //                             }
// //                         ],
// //                         "nextPageToken": ""
// //                     }
// //                 ));
// //         });

// //         let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
// //         let client = DeltaSharingClient::new(profile);
// //         let result = client.list_all_tables("foo").await.unwrap();

// //         mock.assert();
// //         assert_eq!(result.len(), 1);
// //         assert_eq!(result[0].name(), "baz");
// //         assert_eq!(result[0].schema_name(), "qux");
// //         assert_eq!(result[0].share_name(), "quux");
// //         assert_eq!(result[0].share_id(), Some("corge".into()));
// //         assert_eq!(result[0].id(), Some("grault".into()));
// //     }

// //     #[tokio::test]
// //     async fn get_table_metadata() {
// //         let server = MockServer::start();
// //         let mock = server.mock(|when, then| {
// //             when.method(GET)
// //                 .path("/shares/foo/schemas/bar/tables/baz/metadata")
// //                 .header_exists("authorization");
// //             then.status(200)
// //                 .header("content-type", "application/json")
// //                 .header("charset", "utf-8")
// //                 .body_from_file("src/client/metadata_req.txt");
// //         });

// //         let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
// //         let client = DeltaSharingClient::new(profile);
// //         let result = client
// //             .get_table_metadata("foo.bar.baz".parse().unwrap())
// //             .await
// //             .unwrap();

// //         mock.assert();
// //         assert_eq!(result.0, Protocol::default());
// //     }
// // }
