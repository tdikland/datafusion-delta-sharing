use core::fmt;
use std::clone;

use async_trait::async_trait;
use reqwest::{Client, RequestBuilder};
use serde::Serialize;

use crate::auth::Profile;

use super::{
    error::RestClientError,
    request::{IntoRequest, Request},
    response::{ErrorResponse, FromResponse},
};

const CAPABILITIES_HEADER: &str = "delta-sharing-capabilities";
const CAPABILITIES: &str = "responseFormat=parquet";

static USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

#[derive(Clone)]
pub struct RestClient {
    inner: Client,
    profile: Profile,
}

impl fmt::Debug for RestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestClient")
            .field("profile", &self.profile)
            .finish()
    }
}

impl RestClient {
    pub fn new(profile: Profile) -> Self {
        let client = Client::builder().user_agent(USER_AGENT).build().unwrap();
        Self {
            inner: client,
            profile,
        }
    }

    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    pub async fn send_other<R: IntoRequest>(
        &self,
        request: R,
    ) -> Result<R::Response, RestClientError>
    where
        RestClientError: From<R::Error>,
        R::Body: Into<reqwest::Body>,
    {
        let mut req: reqwest::Request = request.into_request()?.try_into().unwrap();

        todo!()
    }

    pub(crate) async fn send<R: Request>(&self, request: R) -> Result<R::Response, RestClientError>
    where
        RestClientError: From<<<R as Request>::Response as FromResponse>::Error>,
    {
        let url = if self.profile.endpoint().as_ref().ends_with('/') {
            format!(
                "{}{}",
                self.profile.endpoint(),
                request.endpoint().to_string().strip_prefix('/').unwrap()
            )
        } else {
            format!("{}{}", self.profile.endpoint(), request.endpoint())
        };

        tracing::info!(url = url, method = ?R::HTTP_METHOD, "send request");
        let response = self
            .inner
            .request(R::HTTP_METHOD, &url)
            .headers(request.headers())
            .header(CAPABILITIES_HEADER, CAPABILITIES)
            .query(&request.query())
            .with_body(request.body())
            .with_auth(&self.profile)
            .await
            .send()
            .await?;

        if !response.status().is_success() {
            tracing::error!(res = ?response, "error response");
            let status = response.status();
            let error_response = response
                .json::<ErrorResponse>()
                .await
                .map_err(|e| RestClientError::DecodeErrorResponse(e.to_string()))?;

            Err(RestClientError::ErrorResponse {
                status,
                body: error_response,
            })
        } else {
            tracing::info!(res = ?response, "success response");
            let parsed_response = R::Response::parse(response).await?;
            Ok(parsed_response)
        }
    }

    // pub async fn list_shares_paginated(
    //     &self,
    //     max_results: Option<i32>,
    //     page_token: Option<String>,
    // ) -> Result<ListSharesResponse, RestClientError> {
    //     let request = ListSharesRequest::builder()
    //         .maybe_max_results(max_results)
    //         .maybe_page_token(page_token)
    //         .build();
    //     self.send(request).await
    // }

    // pub async fn get_share(&self, share: String) -> Result<GetShareResponse, RestClientError> {
    //     let request = GetShareRequest::builder().share_name(share).build();
    //     self.send(request).await
    // }

    // pub async fn list_schemas(
    //     &self,
    //     share: String,
    //     max_results: Option<i32>,
    //     page_token: Option<String>,
    // ) -> Result<ListSchemasResponse, RestClientError> {
    //     let request = ListSchemasRequest::builder()
    //         .share_name(share)
    //         .maybe_max_results(max_results)
    //         .maybe_page_token(page_token)
    //         .build();
    //     self.send(request).await
    // }

    // pub async fn list_tables_in_schema(
    //     &self,
    //     share: String,
    //     schema: String,
    //     max_results: Option<i32>,
    //     page_token: Option<String>,
    // ) -> Result<ListTablesResponse, RestClientError> {
    //     let request = ListTablesInSchemaRequest::builder()
    //         .share_name(share)
    //         .schema_name(schema)
    //         .maybe_max_results(max_results)
    //         .maybe_page_token(page_token)
    //         .build();
    //     self.send(request).await
    // }

    // pub async fn list_tables_in_share(
    //     &self,
    //     share: String,
    //     max_results: Option<i32>,
    //     page_token: Option<String>,
    // ) -> Result<ListTablesResponse, RestClientError> {
    //     let request = ListTablesInShareRequest::builder()
    //         .share_name(share)
    //         .maybe_max_results(max_results)
    //         .maybe_page_token(page_token)
    //         .build();
    //     self.send(request).await
    // }

    // pub async fn query_table_version(
    //     &self,
    //     share_name: String,
    //     schema_name: String,
    //     table_name: String,
    //     starting_timestamp: Option<String>,
    // ) -> Result<QueryTableVersionResponse, RestClientError> {
    //     let request = QueryTableVersionRequest::builder()
    //         .share_name(share_name)
    //         .schema_name(schema_name)
    //         .table_name(table_name)
    //         .maybe_starting_timestamp(starting_timestamp)
    //         .build();
    //     self.send(request).await
    // }
}

#[async_trait]
trait RequestBuilderExt: Sized {
    fn with_body<T: Serialize>(self, body: Option<T>) -> Self;

    async fn with_auth(self, auth: &Profile) -> Self;
}

#[async_trait]
impl RequestBuilderExt for RequestBuilder {
    fn with_body<T: Serialize>(self, body: Option<T>) -> Self {
        match body {
            Some(b) => self.json(&b),
            None => self,
        }
    }

    // TODO: propagate result for expired token, failed to fetch new, etc.
    async fn with_auth(self, auth: &Profile) -> Self {
        let token = auth.get_token().await.unwrap();
        self.bearer_auth(token)
    }
}

#[cfg(test)]
mod test {
    use httpmock::MockServer;
    use serde_json::json;
    use tracing_test::traced_test;
    use url::Url;

    use super::super::request::{GetShareRequest, ListSharesRequest};
    use crate::{auth::ProfileType, model::ShareInfo};

    use super::*;

    #[traced_test]
    #[tokio::test]
    async fn list_shares() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares")
                .query_param("maxResults", "100")
                .query_param("pageToken", "token")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json; charset=utf-8")
                .json_body(json!({
                    "items": [
                        {"name": "foo", "id": "1"},
                        {"name": "bar", "id": "2"}
                    ],
                    "nextPageToken": "next_token"
                }));
        });
        let client = build_sharing_client(&server);

        let req = ListSharesRequest::builder()
            .max_results(100)
            .page_token(String::from("token"))
            .build();
        let res = client.send(req).await.unwrap();

        mock.assert();
        assert_eq!(res.items.len(), 2);
        assert_eq!(
            res.items[0],
            ShareInfo::builder().name("foo").id("1").build()
        );
        assert_eq!(
            res.items[1],
            ShareInfo::builder().name("bar").id("2").build()
        );
        assert_eq!(res.next_page_token, Some("next_token".to_owned()));
    }

    #[traced_test]
    #[tokio::test]
    #[ignore = "todo"]
    async fn get_share() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares/vaccine_share")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json; charset=utf-8")
                .body_from_file("./src/client/resources/get_share.json");
        });
        let client = build_sharing_client(&server);

        let req = GetShareRequest::builder()
            .share_name("vaccine_share".to_string())
            .build();
        let res = client.send(req).await.unwrap();

        mock.assert();
        // assert_eq!(result, share);
        assert!(false)
    }

    #[traced_test]
    #[tokio::test]
    #[ignore = "todo"]
    async fn list_schemas() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares")
                // TODO check query params .query()
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json; charset=utf-8")
                .body_from_file("./src/client/resources/list_shares.json");
        });
        let client = build_sharing_client(&server);

        let req = ListSharesRequest::builder().build();
        let res = client.send(req).await.unwrap();

        mock.assert();
        // assert_eq!(result, share);
        assert!(false)
    }

    #[traced_test]
    #[tokio::test]
    #[ignore = "todo"]
    async fn list_tables_in_share() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares")
                // TODO check query params .query()
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json; charset=utf-8")
                .body_from_file("./src/client/resources/list_shares.json");
        });
        let client = build_sharing_client(&server);

        let req = ListSharesRequest::builder().build();
        let res = client.send(req).await.unwrap();

        mock.assert();
        // assert_eq!(result, share);
        assert!(false)
    }

    #[traced_test]
    #[tokio::test]
    #[ignore = "todo"]
    async fn list_tables_in_schema() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares")
                // TODO check query params .query()
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json; charset=utf-8")
                .body_from_file("./src/client/resources/list_shares.json");
        });
        let client = build_sharing_client(&server);

        let req = ListSharesRequest::builder().build();
        let res = client.send(req).await.unwrap();

        mock.assert();
        // assert_eq!(result, share);
        assert!(false)
    }

    #[traced_test]
    #[tokio::test]
    #[ignore = "todo"]
    async fn query_table_version() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares")
                // TODO check query params .query()
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json; charset=utf-8")
                .body_from_file("./src/client/resources/list_shares.json");
        });
        let client = build_sharing_client(&server);

        let req = ListSharesRequest::builder().build();
        let res = client.send(req).await.unwrap();

        mock.assert();
        // assert_eq!(result, share);
        assert!(false)
    }

    #[traced_test]
    #[tokio::test]
    #[ignore = "todo"]
    async fn query_table_metadata() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares")
                // TODO check query params .query()
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json; charset=utf-8")
                .body_from_file("./src/client/resources/list_shares.json");
        });
        let client = build_sharing_client(&server);

        let req = ListSharesRequest::builder().build();
        let res = client.send(req).await.unwrap();

        mock.assert();
        // assert_eq!(result, share);
        assert!(false)
    }

    #[traced_test]
    #[tokio::test]
    #[ignore = "todo"]
    async fn query_table_data() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares")
                // TODO check query params .query()
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json; charset=utf-8")
                .body_from_file("./src/client/resources/list_shares.json");
        });
        let client = build_sharing_client(&server);

        let req = ListSharesRequest::builder().build();
        let res = client.send(req).await.unwrap();

        mock.assert();
        // assert_eq!(result, share);
        assert!(false)
    }

    #[traced_test]
    #[tokio::test]
    #[ignore = "todo"]
    async fn query_table_changes() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares")
                // TODO check query params .query()
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json; charset=utf-8")
                .body_from_file("./src/client/resources/list_shares.json");
        });
        let client = build_sharing_client(&server);

        let req = ListSharesRequest::builder().build();
        let res = client.send(req).await.unwrap();

        mock.assert();
        // assert_eq!(result, share);
        assert!(false)
    }

    fn build_sharing_client(server: &MockServer) -> RestClient {
        let profile_type = ProfileType::new_bearer_token("test-token", None);
        let mock_server_url = server.base_url().parse::<Url>().unwrap();
        let profile: Profile = Profile::from_profile_type(1, mock_server_url, profile_type);
        RestClient::new(profile)
    }
}

// #[cfg(test)]
// mod test {
//     use chrono::{TimeZone, Utc};
//     use httpmock::MockServer;
//     use serde_json::json;
//     use tracing_test::traced_test;

//     use crate::profile::ProfileType;

//     use super::*;

//     #[test]
//     fn test_url_for_share() {
//         let endpoint = Url::parse("https://example.com/prefix").unwrap();
//         let share = Share::new("my-share", None);
//         let url = url_for_share(endpoint.clone(), &share, None);
//         assert_eq!(url.as_str(), "https://example.com/prefix/shares/my-share");

//         let url = url_for_share(endpoint.clone(), &share, Some("res"));
//         assert_eq!(
//             url.as_str(),
//             "https://example.com/prefix/shares/my-share/res"
//         );
//     }

//     #[test]
//     fn test_url_for_schema() {
//         let endpoint = Url::parse("https://example.com/prefix/").unwrap();
//         let schema = Schema::new("my-share", "my-schema");
//         let url = url_for_schema(endpoint.clone(), &schema, None);
//         assert_eq!(
//             url.as_str(),
//             "https://example.com/prefix/shares/my-share/schemas/my-schema"
//         );

//         let url = url_for_schema(endpoint.clone(), &schema, Some("res"));
//         assert_eq!(
//             url.as_str(),
//             "https://example.com/prefix/shares/my-share/schemas/my-schema/res"
//         );
//     }

//     #[test]
//     fn test_url_for_table() {
//         let endpoint = Url::parse("https://example.com/prefix").unwrap();
//         let table = Table::new("my-share", "my-schema", "my-table", None, None);
//         let url = url_for_table(endpoint.clone(), &table, None);
//         assert_eq!(
//             url.as_str(),
//             "https://example.com/prefix/shares/my-share/schemas/my-schema/tables/my-table"
//         );

//         let url = url_for_table(endpoint.clone(), &table, Some("res"));
//         assert_eq!(
//             url.as_str(),
//             "https://example.com/prefix/shares/my-share/schemas/my-schema/tables/my-table/res"
//         );
//     }

//     fn build_sharing_client(server: &MockServer) -> DeltaSharingClient {
//         let profile_type = ProfileType::new_bearer_token("test-token", None);
//         let mock_server_url = server.base_url().parse::<Url>().unwrap();
//         let profile: Profile = Profile::from_profile_type(1, mock_server_url, profile_type);
//         DeltaSharingClient::new(profile)
//     }

//     #[traced_test]
//     #[tokio::test]
//     async fn list_shares_paginated() {
//         let server = MockServer::start();
//         let mock = server.mock(|when, then| {
//             when.method("GET")
//                 .path("/shares")
//                 .query_param("maxResults", "1")
//                 .query_param("pageToken", "foo")
//                 .header_exists("authorization");
//             then.status(200)
//                 .header("content-type", "application/json")
//                 .header("charset", "utf-8")
//                 .body_from_file("./src/client/resources/list_shares.json");
//         });
//         let client = build_sharing_client(&server);

//         let result = client
//             .list_shares_paginated(&Pagination::start(Some(1), Some("foo".into())))
//             .await
//             .unwrap();

//         mock.assert();
//         assert_eq!(
//             result.items(),
//             vec![
//                 Share::new(
//                     "vaccine_share",
//                     Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
//                 ),
//                 Share::new("sales_share", Some("3e979c79-6399-4dac-bcf8-54e268f48515"))
//             ]
//         );
//         assert_eq!(result.next_page_token(), Some("..."));
//     }

//     #[traced_test]
//     #[tokio::test]
//     async fn get_share() {
//         let server = MockServer::start();
//         let mock = server.mock(|when, then| {
//             when.method("GET")
//                 .path("/shares/vaccine_share")
//                 .header_exists("authorization");
//             then.status(200)
//                 .header("content-type", "application/json")
//                 .header("charset", "utf-8")
//                 .body_from_file("./src/client/resources/get_share.json");
//         });
//         let client = build_sharing_client(&server);
//         let share = Share::new(
//             "vaccine_share",
//             Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f"),
//         );

//         let result = client.get_share(&share).await.unwrap();

//         mock.assert();
//         assert_eq!(result, share);
//     }

//     #[traced_test]
//     #[tokio::test]
//     async fn list_schemas_paginated() {
//         let server = MockServer::start();
//         let mock = server.mock(|when, then| {
//             when.method("GET")
//                 .path("/shares/vaccine_share/schemas")
//                 .query_param("maxResults", "1")
//                 .query_param("pageToken", "foo")
//                 .header_exists("authorization");
//             then.status(200)
//                 .header("content-type", "application/json")
//                 .header("charset", "utf-8")
//                 .body_from_file("./src/client/resources/list_schemas.json");
//         });
//         let client = build_sharing_client(&server);
//         let share = Share::new("vaccine_share", None);

//         let result = client
//             .list_schemas_paginated(&share, &Pagination::start(Some(1), Some("foo".into())))
//             .await
//             .unwrap();

//         mock.assert();
//         assert_eq!(
//             result.items(),
//             vec![Schema::new("vaccine_share", "acme_vaccine_data")]
//         );
//         assert_eq!(result.next_page_token(), Some("..."));
//     }

//     #[traced_test]
//     #[tokio::test]
//     async fn list_tables_in_schema_paginated() {
//         let server = MockServer::start();
//         let mock = server.mock(|when, then| {
//             when.method("GET")
//                 .path("/shares/vaccine_share/schemas/acme_vaccine_data/tables")
//                 .query_param("maxResults", "1")
//                 .query_param("pageToken", "foo")
//                 .header_exists("authorization");
//             then.status(200)
//                 .header("content-type", "application/json")
//                 .header("charset", "utf-8")
//                 .body_from_file("./src/client/resources/list_tables_in_schema.json");
//         });
//         let client = build_sharing_client(&server);
//         let schema = Schema::new("vaccine_share", "acme_vaccine_data");

//         let result = client
//             .list_tables_in_schema_paginated(
//                 &schema,
//                 &Pagination::start(Some(1), Some("foo".into())),
//             )
//             .await
//             .unwrap();

//         mock.assert();
//         assert_eq!(
//             result.items(),
//             vec![
//                 Table::new(
//                     "vaccine_share",
//                     "acme_vaccine_data",
//                     "vaccine_ingredients",
//                     Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".into()),
//                     Some("dcb1e680-7da4-4041-9be8-88aff508d001".into())
//                 ),
//                 Table::new(
//                     "vaccine_share",
//                     "acme_vaccine_data",
//                     "vaccine_patients",
//                     Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".into()),
//                     Some("c48f3e19-2c29-4ea3-b6f7-3899e53338fa".into())
//                 )
//             ]
//         );
//         assert_eq!(result.next_page_token(), Some("..."));
//     }

//     #[traced_test]
//     #[tokio::test]
//     async fn list_tables_in_share_paginated() {
//         let server = MockServer::start();
//         let mock = server.mock(|when, then| {
//             when.method("GET")
//                 .path("/shares/vaccine_share/all-tables")
//                 .query_param("maxResults", "1")
//                 .query_param("pageToken", "foo")
//                 .header_exists("authorization");
//             then.status(200)
//                 .header("content-type", "application/json")
//                 .header("charset", "utf-8")
//                 .body_from_file("./src/client/resources/list_tables_in_share.json");
//         });
//         let client = build_sharing_client(&server);
//         let share = Share::new("vaccine_share", None);

//         let result = client
//             .list_tables_in_share_paginated(&share, &Pagination::start(Some(1),
// Some("foo".into())))             .await
//             .unwrap();

//         mock.assert();
//         assert_eq!(
//             result.items(),
//             vec![
//                 Table::new(
//                     "vaccine_share",
//                     "acme_vaccine_ingredient_data",
//                     "vaccine_ingredients",
//                     Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".into()),
//                     Some("2f9729e9-6fcf-4d34-96df-bf72b26dfbe9".into())
//                 ),
//                 Table::new(
//                     "vaccine_share",
//                     "acme_vaccine_patient_data",
//                     "vaccine_patients",
//                     Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".into()),
//                     Some("74be6365-0fc8-4a2f-8720-0de125bb5832".into())
//                 )
//             ]
//         );
//         assert_eq!(result.next_page_token(), Some("..."));
//     }

//     #[traced_test]
//     #[tokio::test]
//     async fn get_table_version() {
//         let server = MockServer::start();
//         let mock = server.mock(|when, then| {
//             when.method("GET")
//
// .path("/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/version")
//                 .query_param("startingTimestamp", "2022-01-01T00:00:00Z")
//                 .header_exists("authorization");
//             then.status(200)
//                 .header("delta-table-version", "123")
//                 .header("content-type", "application/json")
//                 .header("charset", "utf-8")
//                 .body("");
//         });
//         let client = build_sharing_client(&server);
//         let table = Table::new(
//             "vaccine_share",
//             "acme_vaccine_data",
//             "vaccine_patients",
//             None,
//             None,
//         );
//         let starting_timestamp = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();

//         let result = client
//             .get_table_version(&table, Some(starting_timestamp))
//             .await
//             .unwrap();

//         mock.assert();
//         assert_eq!(result, 123);
//     }

//     #[traced_test]
//     #[tokio::test]
//     async fn get_table_metadata() {
//         let server = MockServer::start();
//         let mock = server.mock(|when, then| {
//             when.method("GET")
//
// .path("/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/metadata")
//                 .header_exists("authorization");
//             then.status(200)
//                 .header("content-type", "application/x-ndjson")
//                 .header("charset", "utf-8")
//                 .header("delta-table-version", "123")
//                 .body_from_file("./src/client/resources/get_table_metadata.ndjson");
//         });
//         let client = build_sharing_client(&server);
//         let table = Table::new(
//             "vaccine_share",
//             "acme_vaccine_data",
//             "vaccine_patients",
//             None,
//             None,
//         );

//         let (protocol, metadata) = client.get_table_metadata(&table).await.unwrap();

//         mock.assert();
//         assert_eq!(protocol.min_reader_version(), 1);
//         assert_eq!(metadata.id(), "table_id");
//         assert_eq!(metadata.format().provider(), "parquet");
//         assert_eq!(metadata.schema_string(), "schema_as_string");
//         assert_eq!(metadata.partition_columns(), &["date"]);
//     }

//     #[traced_test]
//     #[tokio::test]
//     async fn get_table_data() {
//         let server = MockServer::start();
//         let mock = server.mock(|when, then| {
//             when.method("POST")
//                 .path(
//
// "/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/query",
// )                 .json_body(json!({
//                     "jsonPredicateHints": "{\"foo\": \"bar\"}",
//                     "limitHint": "100"
//                 }))
//                 .header_exists("authorization");
//             then.status(200)
//                 .header("content-type", "application/x-ndjson")
//                 .header("charset", "utf-8")
//                 .header("delta-table-version", "123")
//                 .body_from_file("./src/client/resources/get_table_data.ndjson");
//         });
//         let client = build_sharing_client(&server);
//         let table = Table::new(
//             "vaccine_share",
//             "acme_vaccine_data",
//             "vaccine_patients",
//             None,
//             None,
//         );

//         let result = client
//             .get_table_data(&table, Some("{\"foo\": \"bar\"}".into()), Some(100))
//             .await
//             .unwrap();

//         mock.assert();
//         assert_eq!(result.len(), 2);
//     }

//     #[traced_test]
//     #[tokio::test]
//     async fn get_table_data_not_found() {
//         let server = MockServer::start();
//         let mock = server.mock(|when, then| {
//             when.method("POST")
//                 .path("/shares/my_share/schemas/my_schema/tables/fake_table/query");
//             then.status(404)
//                 .header("content-type", "application/json")
//                 .header("charset", "utf-8")
//                 .body(r#"{"errorCode": "RESOURCE_DOES_NOT_EXIST", "message":
// "[Share/Schema/Table] 'my_share/my_schema/fake_table' does not exist, please contact your share
// provider for further information."}"#);         });
//         let client = build_sharing_client(&server);
//         let table = "my_share.my_schema.fake_table".parse::<Table>().unwrap();

//         let result = client.get_table_data(&table, None, None).await;

//         mock.assert();
//         assert!(result.is_err());
//         assert!(result.unwrap_err().to_string().starts_with("[SHARING_CLIENT_ERROR] [RESOURCE_DOES_NOT_EXIST] [Share/Schema/Table] 'my_share/my_schema/fake_table' does not exist"));
//     }
// }
