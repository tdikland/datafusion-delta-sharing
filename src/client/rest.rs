use std::time::Duration;

use bytes::Bytes;
use reqwest::{Client, ClientBuilder, Error, Method, RequestBuilder, Response, StatusCode, Url};
use serde::{de::DeserializeOwned, Deserialize};

use tracing::debug;

use super::{
    profile::DeltaSharingProfile,
    response::{
        ErrorResponse, ListAllTablesPaginated, ListSchemasPaginated, ListSharesPaginated,
        ListTablesPaginated,
    },
    securable::{Schema, Share, Table},
};

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ServerResponse<T> {
    Error(ErrorResponse),
    Succes(T),
}

impl<T> ServerResponse<T> {
    pub fn with_status(self, status: StatusCode) -> Result<T, ClientError> {
        match self {
            ServerResponse::Error(e) => Err(ClientError::status(status, e)),
            ServerResponse::Succes(t) => Ok(t),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RestClient {
    client: Client,
    profile: DeltaSharingProfile,
    endpoint: Url,
}

#[derive(Debug, Deserialize)]
pub enum ClientError {
    Other(String),
}

impl ClientError {
    fn status(status: reqwest::StatusCode, error: ErrorResponse) -> Self {
        Self::Other(format!("ConnectionError: {}; message: {:?}", status, error))
    }

    fn send(error: reqwest::Error) -> Self {
        Self::Other(format!("ConnectionError: {}", error))
    }

    fn decode(error: reqwest::Error) -> Self {
        Self::Other(format!("DecodeError: {}", error))
    }
}

#[derive(Debug)]
pub struct Pagination {
    max_results: Option<u32>,
    page_token: Option<String>,
    is_start: bool,
}

impl Pagination {
    const QUERY_PARAM_MAX_RESULTS: &'static str = "maxResults";
    const QUERY_PARAM_PAGE_TOKEN: &'static str = "pageToken";

    fn new(max_results: Option<u32>, page_token: Option<String>, is_start: bool) -> Self {
        Self {
            max_results,
            page_token,
            is_start,
        }
    }

    fn set_next_page_token(&mut self, token: Option<String>) {
        self.is_start = false;
        self.page_token = token;
    }

    fn has_next_page(&self) -> bool {
        self.is_start || self.page_token.is_some()
    }

    fn is_finished(&self) -> bool {
        !self.has_next_page()
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self::new(None, None, true)
    }
}

impl RestClient {
    pub fn new(profile: DeltaSharingProfile) -> Self {
        Self {
            client: Client::new(),
            profile: profile.clone(),
            endpoint: profile.url(),
        }
    }

    pub async fn list_shares_paginated(
        &self,
        pagination: &Pagination,
    ) -> Result<ListSharesPaginated, ClientError> {
        let mut url = self.endpoint.clone();
        url.set_path(&format!("/shares"));
        url.set_pagination(&pagination);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();
        response
            .json::<ServerResponse<ListSharesPaginated>>()
            .await
            .map_err(ClientError::decode)?
            .with_status(status)
    }

    pub async fn list_shares(&self) -> Result<Vec<Share>, ClientError> {
        let mut shares = vec![];
        let mut pagination = Pagination::default();
        loop {
            let response = self.list_shares_paginated(&pagination).await?;
            pagination.set_next_page_token(response.next_page_token.clone());
            shares.extend(response);
            if pagination.is_finished() {
                break;
            }
        }
        Ok(shares)
    }

    pub async fn list_schemas_paginated(
        &self,
        share: &str,
        pagination: &Pagination,
    ) -> Result<ListSchemasPaginated, ClientError> {
        let mut url = self.endpoint.clone();
        url.set_path(&format!("/shares/{}/schemas", share));
        url.set_pagination(pagination);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();
        response
            .json::<ServerResponse<ListSchemasPaginated>>()
            .await
            .map_err(ClientError::decode)?
            .with_status(status)
    }

    pub async fn list_schemas(&self, share: &str) -> Result<Vec<Schema>, ClientError> {
        let mut schemas = vec![];
        let mut pagination = Pagination::default();
        loop {
            let response = self.list_schemas_paginated(share, &pagination).await?;
            pagination.set_next_page_token(response.next_page_token.clone());
            schemas.extend(response);
            if pagination.is_finished() {
                break;
            }
        }
        Ok(schemas)
    }

    pub async fn list_tables_paginated(
        &self,
        share: &str,
        schema: &str,
        pagination: &Pagination,
    ) -> Result<ListTablesPaginated, ClientError> {
        let mut url = self.endpoint.clone();
        url.set_path(&format!("/shares/{}/schemas/{}/tables", share, schema));
        url.set_pagination(pagination);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();
        response
            .json::<ServerResponse<ListTablesPaginated>>()
            .await
            .map_err(ClientError::decode)?
            .with_status(status)
    }

    pub async fn list_tables(&self, share: &str, schema: &str) -> Result<Vec<Table>, ClientError> {
        let mut tables = vec![];
        let mut pagination = Pagination::default();
        loop {
            let response = self
                .list_tables_paginated(share, schema, &pagination)
                .await?;
            pagination.set_next_page_token(response.next_page_token.clone());
            tables.extend(response);
            if pagination.is_finished() {
                break;
            }
        }
        Ok(tables)
    }

    async fn list_all_tables_paginated(
        &self,
        share: &str,
        pagination: &Pagination,
    ) -> Result<ListAllTablesPaginated, ClientError> {
        let mut url = self.endpoint.clone();
        url.set_path(&format!("/shares/{}/all-tables", share));
        url.set_pagination(pagination);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();
        response
            .json::<ServerResponse<ListAllTablesPaginated>>()
            .await
            .map_err(ClientError::decode)?
            .with_status(status)
    }

    pub async fn list_all_tables(&self, share: &str) -> Result<Vec<Table>, ClientError> {
        let mut tables = vec![];
        let mut pagination = Pagination::default();
        loop {
            let response = self.list_all_tables_paginated(share, &pagination).await?;
            pagination.set_next_page_token(response.next_page_token.clone());
            tables.extend(response);
            if pagination.is_finished() {
                break;
            }
        }
        Ok(tables)
    }

    async fn get_table_version(&self, share: &str, schema: &str, table: &str) {
        let mut url = self
            .endpoint
            .clone()
            .path_segments_mut()
            .expect("URL is a base")
            .push("shares")
            .push(share)
            .push("schema")
            .push(schema)
            .push("table")
            .push(table)
            .push("version");
    }

    async fn get_table_metadata(&self, share: &str, schema: &str, table: &str) {
        todo!()
    }

    async fn get_table_data(&self, share: &str, schema: &str, table: &str) {
        todo!()
    }

    async fn get_table_changes(&self, share: &str, schema: &str, table: &str) {
        todo!()
    }

    async fn request(&self, method: Method, url: Url) -> Result<Response, ClientError> {
        self.client
            .request(method, url)
            .bearer_auth(self.profile.token())
            .send()
            .await
            .map_err(ClientError::send)
    }
}

trait PaginationExt {
    fn set_pagination(&mut self, pagination: &Pagination);
}

impl PaginationExt for Url {
    fn set_pagination(&mut self, pagination: &Pagination) {
        let mut query_pairs = self.query_pairs_mut();
        if let Some(m) = pagination.max_results {
            query_pairs.append_pair(Pagination::QUERY_PARAM_MAX_RESULTS, &m.to_string());
        };
        if let Some(token) = &pagination.page_token {
            query_pairs.append_pair(Pagination::QUERY_PARAM_PAGE_TOKEN, &token);
        };
        drop(query_pairs);
    }
}

async fn handle_response() {
    todo!()
}

#[cfg(test)]
mod test {
    use super::*;
    use httpmock::prelude::*;
    use serde_json::json;

    #[tokio::test]
    async fn list_shares_paginated() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/shares")
                .query_param("maxResults", "1")
                .query_param("pageToken", "foo")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .json_body(json!(
                    {
                        "items": [
                            {
                                "name": "foo",
                                "id": "bar"
                            }
                        ],
                        "nextPageToken": "baz"
                    }
                ));
        });

        let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
        let client = RestClient::new(profile);
        let result = client
            .list_shares_paginated(&Pagination::new(Some(1), Some("foo".into())))
            .await
            .unwrap();

        mock.assert();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].name(), "foo");
        assert_eq!(result.items[0].id(), Some("bar".into()));
        assert_eq!(result.next_page_token, Some("baz".into()));
    }

    #[tokio::test]
    async fn unauthenticated() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/shares")
                .query_param("maxResults", "1")
                .query_param("pageToken", "foo")
                .header_exists("authorization");
            then.status(401)
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .json_body(json!(
                    {
                        "errorCode": "UNAUTHENTICATED",
                        "message": "The request was not authenticated"
                    }
                ));
        });

        let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
        let client = RestClient::new(profile);
        let result = client
            .list_shares_paginated(&Pagination::new(Some(1), Some("foo".into())))
            .await
            .unwrap();

        mock.assert();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].name(), "foo");
        assert_eq!(result.items[0].id(), Some("bar".into()));
        assert_eq!(result.next_page_token, Some("baz".into()));
    }

    #[tokio::test]
    async fn list_shares() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/shares")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .json_body(json!(
                    {
                        "items": [
                            {
                                "name": "foo",
                                "id": "bar"
                            }
                        ],
                        "nextPageToken": ""
                    }
                ));
        });

        let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
        let client = RestClient::new(profile);
        let result = client.list_shares().await.unwrap();

        mock.assert();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Share::new("foo", Some("bar")));
    }

    #[tokio::test]
    async fn list_all_tables_paginated() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/shares/foo/all-tables")
                .query_param("maxResults", "1")
                .query_param("pageToken", "bar")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .json_body(json!(
                    {
                        "items": [
                            {
                                "name": "baz",
                                "schema": "qux",
                                "share": "quux",
                                "shareId": "corge",
                                "id": "grault"
                            }
                        ],
                        "nextPageToken": "garply"
                    }
                ));
        });

        let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
        let client = RestClient::new(profile);
        let result = client
            .list_all_tables_paginated("foo", &Pagination::new(Some(1), Some("bar".into()), true))
            .await
            .unwrap();

        mock.assert();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].name(), "baz");
        assert_eq!(result.items[0].schema_name(), "qux");
        assert_eq!(result.items[0].share_name(), "quux");
        assert_eq!(result.items[0].share_id(), Some("corge".into()));
        assert_eq!(result.items[0].id(), Some("grault".into()));
        assert_eq!(result.next_page_token, Some("garply".into()));
    }

    #[tokio::test]
    async fn list_all_tables() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/shares/foo/all-tables")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .json_body(json!(
                    {
                        "items": [
                            {
                                "name": "baz",
                                "schema": "qux",
                                "share": "quux",
                                "shareId": "corge",
                                "id": "grault"
                            }
                        ],
                        "nextPageToken": ""
                    }
                ));
        });

        let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
        let client = RestClient::new(profile);
        let result = client.list_all_tables("foo").await.unwrap();

        mock.assert();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name(), "baz");
        assert_eq!(result[0].schema_name(), "qux");
        assert_eq!(result[0].share_name(), "quux");
        assert_eq!(result[0].share_id(), Some("corge".into()));
        assert_eq!(result[0].id(), Some("grault".into()));
    }

    // #[ignore]
    // #[tokio::test]
    // async fn list_tables() {
    //     let endpoint = std::env::var("SHARING_ENDPOINT").unwrap();
    //     let token = std::env::var("SHARING_TOKEN").unwrap();
    //     let p = DeltaSharingProfile::new(endpoint, token);
    //     let c = RestClient::new(p);

    //     let r = c
    //         .list_tables("tim_dikland_share", "sse", None, None)
    //         .await
    //         .unwrap();
    //     println!("{r:?}");
    //     assert!(false)
    // }

    // #[tokio::test]
    // async fn list_all_tables() {
    //     let endpoint = std::env::var("SHARING_ENDPOINT").unwrap();
    //     let token = std::env::var("SHARING_TOKEN").unwrap();
    //     let p = DeltaSharingProfile::new(endpoint, token);
    //     let c = RestClient::new(p);

    //     let r = c
    //         .list_all_tables("tim_dikland_share", None, None)
    //         .await
    //         .unwrap();
    //     println!("{r:?}");
    //     assert!(false)
    // }
}
