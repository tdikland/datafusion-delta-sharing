use std::{collections::HashMap, io::BufRead};

use reqwest::{Client, ClientBuilder};
use serde::Serialize;
use serde_json::Deserializer;
use url::Url;

use crate::{
    client::{
        pagination::PaginationExt,
        response::{DeltaAction, ErrorResponse, GetShareResponse, ParquetAction},
    },
    profile::DeltaSharingProfileExt,
    securable::{Schema, Share},
    DeltaSharingError, Profile,
};

use http::{Method, StatusCode};

use super::{
    error::DeltaSharingClientError,
    pagination::Pagination,
    response::{ListSchemasResponse, ListSharesResponse, ListTablesResponse, TableAction},
};

const QUERY_PARAM_VERSION_TIMESTAMP: &str = "startingTimestamp";

pub struct DeltaSharingRestClient {
    client: Client,
    profile: Profile,
}

impl DeltaSharingRestClient {
    pub fn new(profile: Profile) -> Self {
        let client = ClientBuilder::new().build().unwrap();
        Self { client, profile }
    }

    /// List shares with pagination
    pub async fn list_shares(
        &self,
        pagination: &Pagination,
    ) -> Result<ListSharesResponse, DeltaSharingClientError> {
        let endpoint = self
            .profile
            .prefix()
            .join(&format!("/shares"))
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to construct URL");
                DeltaSharingClientError::construction(format!(
                    "failed to construct endpoint URL. Reason: {e}"
                ))
            })?
            .with_pagination(pagination);
        tracing::debug!(endpoint = %endpoint, "endpoint URL constructed");

        let response = self.request(Method::GET, endpoint).await?;
        match response.status() {
            StatusCode::OK => {
                let res = response.json::<ListSharesResponse>().await?;
                tracing::debug!("response parsed");
                Ok(res)
            }
            StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                let err = response.json::<ErrorResponse>().await?;
                tracing::debug!("response parsed");
                Err(DeltaSharingClientError::client(
                    err.error_code(),
                    err.message(),
                ))
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                let err = response.json::<ErrorResponse>().await?;
                tracing::debug!("response parsed");
                Err(DeltaSharingClientError::server(
                    err.error_code(),
                    err.message(),
                ))
            }
            _ => {
                tracing::error!(status_code = %response.status(), "unexpected server status code");
                Err(DeltaSharingClientError::other("unknown server response"))
            }
        }
    }

    /// Retrieve a share
    pub async fn get_share(
        &self,
        share_name: &str,
    ) -> Result<Option<Share>, DeltaSharingClientError> {
        let endpoint = self
            .profile
            .prefix()
            .join(&format!("/shares/{share_name}"))
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to construct URL");
                DeltaSharingClientError::construction(format!(
                    "failed to construct endpoint URL. Reason: {e}"
                ))
            })?;
        tracing::debug!(endpoint = %endpoint, "URL constructed");

        let response = self.get(endpoint).await?;
        match response.status() {
            StatusCode::OK => {
                let res = response.json::<GetShareResponse>().await?;
                tracing::debug!("response parsed");
                Ok(Some(res.share))
            }
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                let err = response.json::<ErrorResponse>().await?;
                tracing::debug!("response parsed");
                Err(DeltaSharingClientError::client(
                    err.error_code(),
                    err.message(),
                ))
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                let err = response.json::<ErrorResponse>().await?;
                tracing::debug!("response parsed");
                Err(DeltaSharingClientError::server(
                    err.error_code(),
                    err.message(),
                ))
            }
            _ => {
                tracing::error!(status_code = %response.status(), "unexpected server status code");
                Err(DeltaSharingClientError::other("unknown server response"))
            }
        }
    }

    // /// List schemas with pagination
    // pub async fn list_schemas_paginated(
    //     &self,
    //     share_name: &str,
    //     pagination: &Pagination,
    // ) -> Result<ListSchemasResponse, DeltaSharingError> {
    //     let endpoint = self
    //         .profile
    //         .prefix()
    //         .join(&format!("/shares/{share_name}/schemas"))
    //         .map_err(|e| {
    //             DeltaSharingError::other(format!("failed to build endpoint URL. Reason: {e}"))
    //         })?
    //         .with_pagination(pagination);
    //     tracing::debug!(endpoint = %endpoint, "construct list shares URL");

    //     let request = self
    //         .client
    //         .get(endpoint)
    //         .authorize_with_profile(&self.profile)
    //         .await?;
    //     let response = request.send().await?;
    //     let status = response.status();
    //     tracing::info!(status_code = %status, "list schemas");

    //     if status.is_success() {
    //         Ok(response.json::<ListSchemasResponse>().await?)
    //     } else {
    //         let err = response.json::<ErrorResponse>().await?;
    //         if status.is_client_error() {
    //             Err(DeltaSharingError::client(err.to_string()))
    //         } else {
    //             Err(DeltaSharingError::server(err.to_string()))
    //         }
    //     }
    // }

    // pub async fn list_tables_in_schema_paginated(
    //     &self,
    //     schema: &Schema,
    //     pagination: &Pagination,
    // ) -> Result<ListTablesResponse, DeltaSharingError> {
    //     let mut url = url_for_schema(self.profile.endpoint().clone(), schema, Some("tables"));
    //     url = url.with_pagination(pagination);
    //     trace!("URL: {}", url);

    //     let response = self.request(Method::GET, url).await?;
    //     let status = response.status();

    //     if status.is_success() {
    //         info!("list tables in schema status: {:?}", status);
    //         Ok(response.json::<ListTablesResponse>().await?)
    //     } else {
    //         warn!("list tables in schema status: {:?}", status);
    //         let err = response.json::<ErrorResponse>().await?;
    //         if status.is_client_error() {
    //             Err(DeltaSharingError::client(err.to_string()))
    //         } else {
    //             Err(DeltaSharingError::server(err.to_string()))
    //         }
    //     }
    // }

    // /// List tables in share with pagination
    // async fn list_tables_in_share_paginated(
    //     &self,
    //     share: &Share,
    //     pagination: &Pagination,
    // ) -> Result<ListTablesResponse, DeltaSharingError> {
    //     let mut url = url_for_share(self.profile.endpoint().clone(), share, Some("all-tables"));
    //     url = url.with_pagination(pagination);
    //     trace!("URL: {}", url);

    //     let response = self.request(Method::GET, url).await?;
    //     let status = response.status();

    //     if status.is_success() {
    //         info!("list tables in share status: {:?}", status);
    //         let res = Ok(response.json::<ListTablesResponse>().await?);
    //         info!(res = ?res, "tables");
    //         res
    //     } else {
    //         warn!("list tables in share status: {:?}", status);
    //         let err = response.json::<ErrorResponse>().await?;
    //         if status.is_client_error() {
    //             Err(DeltaSharingError::client(err.to_string()))
    //         } else {
    //             Err(DeltaSharingError::server(err.to_string()))
    //         }
    //     }
    // }

    /// Retrieve the version of a table
    pub async fn get_table_version(
        &self,
        share: &str,
        schema: &str,
        table: &str,
        starting_timestamp: Option<&str>,
    ) -> Result<u64, DeltaSharingClientError> {
        let mut endpoint = self
            .profile
            .prefix()
            .join(&format!(
                "/shares/{share}/schemas/{schema}/tables/{table}/version"
            ))
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to construct URL");
                DeltaSharingClientError::construction(format!(
                    "failed to construct endpoint URL. Reason: {e}"
                ))
            })?;
        if let Some(ts) = starting_timestamp {
            endpoint
                .query_pairs_mut()
                .append_pair(QUERY_PARAM_VERSION_TIMESTAMP, ts);
        }
        tracing::debug!(endpoint = %endpoint, "URL constructed");

        let request = self
            .client
            .get(endpoint)
            .authorize_with_profile(&self.profile)
            .await
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to authorize request");
                DeltaSharingClientError::token_expired(format!(
                    "failed to authorize request. Reason: {e}"
                ))
            })?;
        tracing::debug!("prepared request");

        let response = request.send().await?;
        let status = response.status();
        tracing::debug!(status_code = %status, "server responded");

        match status {
            StatusCode::OK => response
                .headers()
                .get("Delta-Table-Version")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or(DeltaSharingClientError::other(
                    "parsing delta-table-version failed",
                )),
            StatusCode::BAD_REQUEST
            | StatusCode::UNAUTHORIZED
            | StatusCode::FORBIDDEN
            | StatusCode::NOT_FOUND => Err(DeltaSharingClientError::other("ugh")),
            StatusCode::INTERNAL_SERVER_ERROR => Err(DeltaSharingClientError::other("ugh")),
            _ => {
                tracing::error!(status_code = %status, "unexpected server status code");
                Err(DeltaSharingClientError::other("unknown server response"))
            }
        }
    }

    /// Retrieve the metadata of a table
    pub async fn get_table_metadata(
        &self,
        share: &str,
        schema: &str,
        table: &str,
    ) -> Result<(TableAction, TableAction), DeltaSharingClientError> {
        let endpoint = self
            .profile
            .prefix()
            .join(&format!(
                "/shares/{share}/schemas/{schema}/tables/{table}/metadata"
            ))
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to construct URL");
                DeltaSharingClientError::construction(format!(
                    "failed to construct endpoint URL. Reason: {e}"
                ))
            })?;
        tracing::debug!(endpoint = %endpoint, "URL constructed");

        let request = self
            .client
            .get(endpoint)
            .authorize_with_profile(&self.profile)
            .await
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to authorize request");
                DeltaSharingClientError::token_expired(format!(
                    "failed to authorize request. Reason: {e}"
                ))
            })?;
        tracing::debug!("prepared request");

        let response = request.send().await?;
        let status = response.status();
        tracing::debug!(status_code = %status, "server responded");

        match status {
            StatusCode::OK => {
                let bytes = response.bytes().await.unwrap();
                // bytes.lines().take(2).map(|line| )
                let actions = Deserializer::from_slice(&bytes)
                    .into_iter::<TableAction>()
                    .take(2)
                    .collect::<Result<Vec<_>, serde_json::Error>>()
                    .map_err(|e| DeltaSharingClientError::other("deserialization"))?;
                Ok((actions[0].clone(), actions[1].clone()))
            }
            _ => Err(DeltaSharingClientError::other("ble")),
        }
    }

    /// Retrieve the data of a table
    pub async fn get_table_data(
        &self,
        share: &str,
        schema: &str,
        table: &str,
        predicates: Option<String>,
        limit: Option<u32>,
        version: Option<()>,
    ) -> Result<Vec<TableAction>, DeltaSharingClientError> {
        let endpoint = self
            .profile
            .prefix()
            .join(&format!(
                "/shares/{share}/schemas/{schema}/tables/{table}/query"
            ))
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to construct URL");
                DeltaSharingClientError::construction(format!(
                    "failed to construct endpoint URL. Reason: {e}"
                ))
            })?;
        tracing::debug!(endpoint = %endpoint, "URL constructed");

        let mut body: HashMap<String, String> = HashMap::new();
        if let Some(pred) = predicates {
            body.insert("jsonPredicateHints".to_string(), pred);
        }
        if let Some(lim) = limit {
            body.insert("limitHint".to_string(), lim.to_string());
        }
        tracing::info!(body = ?body, "constructed request body");

        let response = self.request(Method::POST, endpoint).await?;

        // let response = self
        //     .client
        //     .request(Method::POST, url)
        //     .json(&body)
        //     .authorize_with_profile(&self.profile)
        //     .await?
        //     .send()
        //     .await?;
        // let status = response.status();

        match response.status() {
            _ => Ok(vec![]),
        }

        // if !status.is_success() {
        //     warn!("get table data status: {:?}", status);
        //     let err = response.json::<ErrorResponse>().await.unwrap();
        //     if status.is_client_error() {
        //         Err(DeltaSharingError::client(err.to_string()))
        //     } else {
        //         Err(DeltaSharingError::server(err.to_string()))
        //     }
        // } else {
        //     let full = response.bytes().await?;

        //     let text = unsafe { String::from_utf8_unchecked(full.as_ref().to_vec()) };
        //     info!(text = %text, "full text");

        //     let mut lines = Deserializer::from_slice(&full).into_iter::<ParquetResponse>();

        //     let _ = lines
        //         .next()
        //         .and_then(Result::ok)
        //         .and_then(ParquetResponse::to_protocol)
        //         .ok_or(DeltaSharingError::parse_response("parsing protocol failed"))?;
        //     let _ = lines
        //         .next()
        //         .and_then(Result::ok)
        //         .and_then(ParquetResponse::to_metadata)
        //         .ok_or(DeltaSharingError::parse_response("parsing metadata failed"))?;

        //     let mut files = vec![];
        //     for line in lines {
        //         info!(line=?line, "processing line");
        //         let file = line.ok().and_then(ParquetResponse::to_file);
        //         if let Some(f) = file {
        //             files.push(f);
        //         }
        //     }

        //     Ok(files)
        // }
    }

    async fn get(&self, endpoint: Url) -> Result<reqwest::Response, DeltaSharingClientError> {
        let request = self
            .client
            .request(Method::GET, endpoint)
            .authorize_with_profile(&self.profile)
            .await
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to authorize request");
                DeltaSharingClientError::token_expired(format!(
                    "failed to authorize request. Reason: {e}"
                ))
            })?;
        tracing::debug!("prepared request");

        let response = request.send().await?;
        let status = response.status();
        tracing::debug!(status_code = %status, "server responded");

        Ok(response)
    }

    async fn request<T: Serialize>(
        &self,
        method: http::Method,
        endpoint: Url,
        body: Option<&T>,
    ) -> Result<reqwest::Response, DeltaSharingClientError> {
        let base = self.client.request(method, endpoint);

        let with_body = if let Some(b) = body {
            base.json(&b)
        } else {
            base
        };

        let request = with_body
            .authorize_with_profile(&self.profile)
            .await
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to authorize request");
                DeltaSharingClientError::token_expired(format!(
                    "failed to authorize request. Reason: {e}"
                ))
            })?;
        tracing::debug!("prepared request");

        let response = request.send().await?;
        let status = response.status();
        tracing::debug!(status_code = %status, "server responded");

        Ok(response)
    }
}

impl From<reqwest::Error> for DeltaSharingClientError {
    fn from(value: reqwest::Error) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use httpmock::{MockServer, Then, When};
    use url::Url;

    use crate::{client::error::DeltaSharingClientErrorKind, profile::ProfileType};

    use super::*;

    #[tokio::test]
    async fn list_shares_full() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.path("/shares").and(authorized_get);
            then.body_from_file("./src/client/resources/rest_list_shares_full.json")
                .and(list_ok);
        });
        let result = client.list_shares(&Pagination::default()).await.unwrap();

        mock.assert();
        assert_eq!(
            result.items(),
            vec![
                Share::new("product", Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")),
                Share::new("sales", Some("3e979c79-6399-4dac-bcf8-54e268f48515")),
                Share::new("finance", Some("f902aeb2-2466-495b-a1d6-22122f822a43"))
            ]
        );
        assert!(result.next_page_token().is_none());
    }

    #[tokio::test]
    async fn list_shares_first_page() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.path("/shares")
                .query_param("maxResults", "2")
                .and(authorized_get);
            then.body_from_file("./src/client/resources/rest_list_shares_first_page.json")
                .and(list_ok);
        });
        let result = client
            .list_shares(&Pagination::from_start(Some(2)))
            .await
            .unwrap();

        mock.assert();
        assert_eq!(
            result.next_page_token(),
            Some("CgExEg1kZWx0YV9zaGFyaW5nGgdkZWZhdWx0")
        );
        assert_eq!(
            result.items(),
            vec![
                Share::new("product", Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")),
                Share::new("sales", Some("3e979c79-6399-4dac-bcf8-54e268f48515"))
            ]
        );
    }

    #[tokio::test]
    async fn list_shares_last_page() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares")
                .query_param("maxResults", "2")
                .query_param("pageToken", "foo")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json; chatset=utf-8")
                .body_from_file("./src/client/resources/rest_list_shares_last_page.json");
        });
        let result = client
            .list_shares(&Pagination::from_token(Some(2), String::from("foo")))
            .await
            .unwrap();

        mock.assert();
        assert!(result.next_page_token().is_none());
        assert_eq!(
            result.items(),
            vec![Share::new(
                "finance",
                Some("f902aeb2-2466-495b-a1d6-22122f822a43")
            )]
        );
    }

    #[tokio::test]
    async fn list_shares_malformed() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.method("GET").path("/shares");
            then.status(400)
                .header("content-type", "application/json")
                .body_from_file("./src/client/resources/rest_malformed.json");
        });
        let client_err = client
            .list_shares(&Pagination::default())
            .await
            .unwrap_err();

        mock.assert();
        assert!(matches!(
            client_err.kind(),
            DeltaSharingClientErrorKind::ClientError(_)
        ));
        assert_eq!(client_err.message(), "bad");
    }

    #[tokio::test]
    async fn list_shares_unauthenticated() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.method("GET").path("/shares");
            then.status(401)
                .header("content-type", "application/json")
                .body_from_file("./src/client/resources/rest_unauthenticated.json");
        });
        let client_err = client
            .list_shares(&Pagination::default())
            .await
            .unwrap_err();

        mock.assert();
        assert!(matches!(
            client_err.kind(),
            DeltaSharingClientErrorKind::ClientError(_)
        ));
        assert_eq!(client_err.message(), "whois");
    }

    #[tokio::test]
    async fn list_shares_forbidden() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.method("GET").path("/shares");
            then.status(403)
                .header("content-type", "application/json")
                .body_from_file("./src/client/resources/rest_forbidden.json");
        });
        let client_err = client
            .list_shares(&Pagination::default())
            .await
            .unwrap_err();

        mock.assert();
        assert!(matches!(
            client_err.kind(),
            DeltaSharingClientErrorKind::ClientError(_)
        ));
        assert_eq!(client_err.message(), "nope");
    }

    #[tokio::test]
    async fn list_shares_server_error() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.method("GET").path("/shares");
            then.status(500)
                .header("content-type", "application/json")
                .body_from_file("./src/client/resources/rest_server_error.json");
        });
        let server_err = client
            .list_shares(&Pagination::default())
            .await
            .unwrap_err();

        mock.assert();
        assert!(matches!(
            server_err.kind(),
            DeltaSharingClientErrorKind::ServerError(_)
        ));
        assert_eq!(server_err.message(), "sad");
    }

    #[tokio::test]
    async fn get_share() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.method("GET").path("/shares/foo");
            then.status(200)
                .header("content-type", "application/json; chatset=utf-8")
                .body_from_file("./src/client/resources/rest_get_share.json");
        });
        let result = client.get_share("foo").await.unwrap();

        mock.assert();
        assert!(result.is_some());

        let share = result.unwrap();
        assert_eq!(share.name(), "foo");
        assert_eq!(share.id(), Some("69bcc83e-9ca7-4ed9-a645-d977204cd860"));
    }

    #[tokio::test]
    async fn get_share_not_found() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.method("GET").path("/shares/foo");
            then.status(404)
                .header("content-type", "application/json")
                .body_from_file("./src/client/resources/rest_share_not_found.json");
        });
        let result = client.get_share("foo").await.unwrap();

        mock.assert();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn get_table_version() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares/foo/schemas/bar/tables/baz/version")
                .query_param("startingTimestamp", "2022-01-01T00:00:00Z");
            then.status(200).header("delta-table-version", "123");
        });
        let result = client
            .get_table_version("foo", "bar", "baz", Some("2022-01-01T00:00:00Z"))
            .await
            .unwrap();

        mock.assert();
        assert_eq!(result, 123);
    }

    #[tokio::test]
    async fn get_table_metadata_parquet() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares/foo/schemas/bar/tables/baz/metadata");
            then.status(200)
                .header("content-type", "application/x-ndjson; charset=utf-8")
                .header("delta-table-version", "123")
                .body_from_file("./src/client/resources/rest_get_metadata_parquet.ndjson");
        });
        let result = client
            .get_table_metadata("foo", "bar", "baz")
            .await
            .unwrap();

        mock.assert();
        assert!(result.0.is_parquet());
        assert!(result.1.is_parquet());
    }

    #[tokio::test]
    async fn get_table_metadata_delta() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares/foo/schemas/bar/tables/baz/metadata");
            then.status(200)
                .header("content-type", "application/x-ndjson; charset=utf-8")
                .header("delta-table-version", "123")
                .body_from_file("./src/client/resources/rest_get_metadata_delta.ndjson");
        });
        let result = client
            .get_table_metadata("foo", "bar", "baz")
            .await
            .unwrap();

        mock.assert();
        assert!(result.0.is_delta());
        assert!(result.1.is_delta());
    }

    #[tokio::test]
    async fn get_table_data_parquet() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.method("POST")
                .path("/shares/foo/schemas/bar/tables/baz/query");
            then.status(200)
                .header("content-type", "application/x-ndjson; charset=utf-8")
                .header("delta-table-version", "123")
                .body_from_file("./src/client/resources/rest_get_data_parquet.ndjson");
        });
        let result = client
            .get_table_data("foo", "bar", "baz", None, None, None)
            .await
            .unwrap();

        mock.assert();
        assert!(result.iter().all(|act| act.is_parquet()));
        assert_eq!(result.len(), 4);
    }

    #[tokio::test]
    async fn get_table_data_delta() {
        let server = MockServer::start();
        let client = client_for_test(&server);

        let mock = server.mock(|when, then| {
            when.method("POST")
                .path("/shares/foo/schemas/bar/tables/baz/query");
            then.status(200)
                .header("content-type", "application/x-ndjson; charset=utf-8")
                .header("delta-table-version", "123")
                .body_from_file("./src/client/resources/rest_get_data_delta.ndjson");
        });
        let result = client
            .get_table_data("foo", "bar", "baz", None, None, None)
            .await
            .unwrap();

        mock.assert();
        assert!(result.iter().all(|act| act.is_delta()));
        assert_eq!(result.len(), 3);
    }

    fn client_for_test(server: &MockServer) -> DeltaSharingRestClient {
        let profile_type = ProfileType::new_bearer_token("test-token", None);
        let mock_server_url = server.base_url().parse::<Url>().unwrap();
        let profile: Profile = Profile::from_profile_type(1, mock_server_url, profile_type);
        DeltaSharingRestClient::new(profile)
    }

    fn authorized_get(when: When) -> When {
        when.header_exists("authorization").method("GET")
    }

    fn list_ok(then: Then) -> Then {
        then.status(http::StatusCode::OK.as_u16())
            .header("content-type", "application/json; chatset=utf-8")
    }
}
