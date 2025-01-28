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

    use crate::{
        auth::ProfileType,
        model::ShareInfo,
        rest::request::{GetShareRequest, ListSharesRequest},
    };

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
        assert_eq!(res.next_page_token, Some("foo".to_owned()));
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
