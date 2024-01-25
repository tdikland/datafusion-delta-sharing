use reqwest::{Client, Method, Response, StatusCode, Url};
use serde::Deserialize;
use serde_json::Deserializer;

use super::{
    action::{Metadata, Protocol},
    profile::DeltaSharingProfile,
    response::{
        ErrorResponse, ListSchemasPaginated, ListSharesPaginated, ListTablesPaginated,
        ParquetResponse,
    },
    securable::{Schema, Share, Table},
};

const QUERY_PARAM_VERSION_TIMESTAMP: &'static str = "startingTimestamp";

#[derive(Debug, Clone)]
pub struct RestClient {
    client: Client,
    profile: DeltaSharingProfile,
    endpoint: Url,
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
            pagination.set_next_page_token(response.next_page_token().map(ToOwned::to_owned));
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
            pagination.set_next_page_token(response.next_page_token().map(ToOwned::to_owned));
            schemas.extend(response);
            if pagination.is_finished() {
                break;
            }
        }
        Ok(schemas)
    }

    pub async fn list_tables_in_schema_paginated(
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
                .list_tables_in_schema_paginated(share, schema, &pagination)
                .await?;
            pagination.set_next_page_token(response.next_page_token().map(ToOwned::to_owned));
            tables.extend(response);
            if pagination.is_finished() {
                break;
            }
        }
        Ok(tables)
    }

    async fn list_tables_in_share_paginated(
        &self,
        share: &str,
        pagination: &Pagination,
    ) -> Result<ListTablesPaginated, ClientError> {
        let mut url = self.endpoint.clone();
        url.set_path(&format!("/shares/{}/all-tables", share));
        url.set_pagination(pagination);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();
        response
            .json::<ServerResponse<ListTablesPaginated>>()
            .await
            .map_err(ClientError::decode)?
            .with_status(status)
    }

    pub async fn list_all_tables(&self, share: &str) -> Result<Vec<Table>, ClientError> {
        let mut tables = vec![];
        let mut pagination = Pagination::default();
        loop {
            let response = self
                .list_tables_in_share_paginated(share, &pagination)
                .await?;
            pagination.set_next_page_token(response.next_page_token().map(ToOwned::to_owned));
            tables.extend(response);
            if pagination.is_finished() {
                break;
            }
        }
        Ok(tables)
    }

    pub async fn get_table_version(
        &self,
        share: &str,
        schema: &str,
        table: &str,
        starting_timestamp: Option<&str>,
    ) -> Result<u64, ClientError> {
        let mut url = self.endpoint.clone();
        url.set_path(&format!(
            "/shares/{}/schemas/{}/tables/{}/version",
            share, schema, table
        ));
        if let Some(ts) = starting_timestamp {
            url.query_pairs_mut()
                .append_pair(QUERY_PARAM_VERSION_TIMESTAMP, ts);
        }

        let response = self.request(Method::GET, url).await.unwrap();
        response
            .headers()
            .get("Delta-Table-Version")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or(ClientError::version())
    }

    pub async fn get_table_metadata(
        &self,
        share: &str,
        schema: &str,
        table: &str,
    ) -> Result<(Protocol, Metadata), ClientError> {
        let mut url = self.endpoint.clone();
        url.set_path(&format!(
            "/shares/{}/schemas/{}/tables/{}/metadata",
            share, schema, table
        ));

        let response = self.request(Method::GET, url).await?;
        let status = response.status();

        if status != 200 {
            let res = response.json::<ErrorResponse>().await.unwrap();
            Err(ClientError::status(status, res))
        } else {
            let full = response.bytes().await.map_err(ClientError::decode)?;
            let mut lines = Deserializer::from_slice(&full).into_iter::<ParquetResponse>();

            let protocol = lines
                .next()
                .and_then(Result::ok)
                .and_then(ParquetResponse::to_protocol)
                .ok_or(ClientError::Other("no protocol".into()))?;
            let metadata = dbg!(lines.next())
                .and_then(Result::ok)
                .and_then(ParquetResponse::to_metadata)
                .ok_or(ClientError::Other("no metadata".into()))?;

            Ok((protocol, metadata))
        }
    }

    async fn get_table_data(
        &self,
        share: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ParquetResponse>, ClientError> {
        let mut url = self.endpoint.clone();
        url.set_path(&format!(
            "/shares/{}/schemas/{}/tables/{}/metadata",
            share, schema, table
        ));

        let response = self.request(Method::GET, url).await?;
        let status = response.status();

        if status != 200 {
            let res = response.json::<ErrorResponse>().await.unwrap();
            Err(ClientError::status(status, res))
        } else {
            let full = response.bytes().await.map_err(ClientError::decode)?;
            let mut lines = Deserializer::from_slice(&full).into_iter::<ParquetResponse>();

            let protocol = lines
                .next()
                .and_then(Result::ok)
                .and_then(ParquetResponse::to_protocol)
                .ok_or(ClientError::Other("no protocol".into()))?;
            let metadata = dbg!(lines.next())
                .and_then(Result::ok)
                .and_then(ParquetResponse::to_metadata)
                .ok_or(ClientError::Other("no metadata".into()))?;

            let mut files = vec![];
            while let Some(f) = lines.next() {
                let ff = f.map_err(|_| ClientError::Other("bad file".into()))?;
                files.push(ff);
            }

            Ok(files)
        }
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

    fn version() -> Self {
        Self::Other("Version header missing".into())
    }
}

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

    pub fn start(max_results: Option<u32>, page_token: Option<String>) -> Self {
        Self::new(max_results, page_token, true)
    }

    fn set_next_page_token(&mut self, token: Option<String>) {
        self.is_start = false;
        self.page_token = token;
    }

    fn has_next_page(&self) -> bool {
        self.is_start || (self.page_token.is_some() && self.page_token.as_deref() != Some(""))
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
            .list_shares_paginated(&Pagination::start(Some(1), Some("foo".into())))
            .await
            .unwrap();

        mock.assert();
        assert_eq!(result.items().len(), 1);
        assert_eq!(result.items()[0].name(), "foo");
        assert_eq!(result.items()[0].id(), Some("bar".into()));
        assert_eq!(result.next_page_token(), Some("baz".into()));
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
            .list_shares_paginated(&Pagination::start(Some(1), Some("foo".into())))
            .await;

        mock.assert();
        assert!(result.is_err());
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

    // #[tokio::test]
    // async fn list_all_tables_paginated() {
    //     let server = MockServer::start();
    //     let mock = server.mock(|when, then| {
    //         when.method(GET)
    //             .path("/shares/foo/all-tables")
    //             .query_param("maxResults", "1")
    //             .query_param("pageToken", "bar")
    //             .header_exists("authorization");
    //         then.status(200)
    //             .header("content-type", "application/json")
    //             .header("charset", "utf-8")
    //             .json_body(json!(
    //                 {
    //                     "items": [
    //                         {
    //                             "name": "baz",
    //                             "schema": "qux",
    //                             "share": "quux",
    //                             "shareId": "corge",
    //                             "id": "grault"
    //                         }
    //                     ],
    //                     "nextPageToken": "garply"
    //                 }
    //             ));
    //     });

    //     let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
    //     let client = RestClient::new(profile);
    //     let result = client
    //         .list_all_tables_paginated("foo", &Pagination::new(Some(1), Some("bar".into()), true))
    //         .await
    //         .unwrap();

    //     mock.assert();
    //     assert_eq!(result.items.len(), 1);
    //     assert_eq!(result.items[0].name(), "baz");
    //     assert_eq!(result.items[0].schema_name(), "qux");
    //     assert_eq!(result.items[0].share_name(), "quux");
    //     assert_eq!(result.items[0].share_id(), Some("corge".into()));
    //     assert_eq!(result.items[0].id(), Some("grault".into()));
    //     assert_eq!(result.next_page_token, Some("garply".into()));
    // }

    // #[tokio::test]
    // async fn list_all_tables() {
    //     let server = MockServer::start();
    //     let mock = server.mock(|when, then| {
    //         when.method(GET)
    //             .path("/shares/foo/all-tables")
    //             .header_exists("authorization");
    //         then.status(200)
    //             .header("content-type", "application/json")
    //             .header("charset", "utf-8")
    //             .json_body(json!(
    //                 {
    //                     "items": [
    //                         {
    //                             "name": "baz",
    //                             "schema": "qux",
    //                             "share": "quux",
    //                             "shareId": "corge",
    //                             "id": "grault"
    //                         }
    //                     ],
    //                     "nextPageToken": ""
    //                 }
    //             ));
    //     });

    //     let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
    //     let client = RestClient::new(profile);
    //     let result = client.list_all_tables("foo").await.unwrap();

    //     mock.assert();
    //     assert_eq!(result.len(), 1);
    //     assert_eq!(result[0].name(), "baz");
    //     assert_eq!(result[0].schema_name(), "qux");
    //     assert_eq!(result[0].share_name(), "quux");
    //     assert_eq!(result[0].share_id(), Some("corge".into()));
    //     assert_eq!(result[0].id(), Some("grault".into()));
    // }

    #[tokio::test]
    async fn get_table_metadata() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/shares/foo/schemas/bar/tables/baz/metadata")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .body_from_file("src/client/metadata_req.txt");
        });

        let profile = DeltaSharingProfile::new(server.base_url(), "token".to_string());
        let client = RestClient::new(profile);
        let result = client
            .get_table_metadata("foo", "bar", "baz")
            .await
            .unwrap();

        mock.assert();
        assert_eq!(result.0, Protocol::default());
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
