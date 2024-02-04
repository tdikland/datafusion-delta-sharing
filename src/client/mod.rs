use std::collections::HashMap;

use chrono::{DateTime, Utc};
use reqwest::{Client, Method, Response, Url};
use serde_json::Deserializer;
use tracing::{debug, info, trace, warn};

use crate::{
    client::response::GetShareResponse,
    error::DeltaSharingError,
    profile::{DeltaSharingProfileExt, Profile},
    securable::{Schema, Share, Table},
};

use {
    action::{File, Metadata, Protocol},
    pagination::{Pagination, PaginationExt},
    response::{
        ErrorResponse, ListSchemasResponse, ListSharesResponse, ListTablesResponse, ParquetResponse,
    },
};

pub mod action;
pub mod pagination;
pub mod response;

const QUERY_PARAM_VERSION_TIMESTAMP: &'static str = "startingTimestamp";

#[derive(Debug, Clone)]
pub struct DeltaSharingClient {
    client: Client,
    profile: Profile,
}

impl DeltaSharingClient {
    pub fn new(profile: Profile) -> Self {
        Self {
            client: Client::new(),
            profile,
        }
    }

    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    pub async fn list_shares_paginated(
        &self,
        pagination: &Pagination,
    ) -> Result<ListSharesResponse, DeltaSharingError> {
        let mut url = self.profile.endpoint().clone();
        url.path_segments_mut()
            .expect("valid base")
            .pop_if_empty()
            .push("shares");
        url.set_pagination(&pagination);
        trace!("URL: {}", url);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();

        if status.is_success() {
            info!("list shares status: {:?}", status);
            Ok(response.json::<ListSharesResponse>().await?)
        } else {
            warn!("list shares status: {:?}", status);
            let err = response.json::<ErrorResponse>().await?;
            if status.is_client_error() {
                Err(DeltaSharingError::client(err.to_string()))
            } else {
                Err(DeltaSharingError::server(err.to_string()))
            }
        }
    }

    pub async fn list_shares(&self) -> Result<Vec<Share>, DeltaSharingError> {
        let mut shares = vec![];
        let mut pagination = Pagination::default();
        loop {
            let response = self.list_shares_paginated(&pagination).await?;
            pagination.set_next_token(response.next_page_token().map(ToOwned::to_owned));
            shares.extend(response);
            if pagination.is_finished() {
                break;
            }
        }
        Ok(shares)
    }

    pub async fn get_share(&self, share: &Share) -> Result<Share, DeltaSharingError> {
        let url = url_for_share(self.profile.endpoint().clone(), share, None);
        trace!("URL: {}", url);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();

        if status.is_success() {
            info!("get share status: {:?}", status);
            let res = response.json::<GetShareResponse>().await?;
            Ok(res.share().clone())
        } else {
            warn!("get share status: {:?}", status);
            let err = response.json::<ErrorResponse>().await?;
            if status.is_client_error() {
                Err(DeltaSharingError::client(err.to_string()))
            } else {
                Err(DeltaSharingError::server(err.to_string()))
            }
        }
    }

    pub async fn list_schemas_paginated(
        &self,
        share: &Share,
        pagination: &Pagination,
    ) -> Result<ListSchemasResponse, DeltaSharingError> {
        let mut url = url_for_share(self.profile.endpoint().clone(), share, Some("schemas"));
        url.set_pagination(pagination);
        trace!("URL: {}", url);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();

        if status.is_success() {
            info!("list schemas status: {:?}", status);
            Ok(response.json::<ListSchemasResponse>().await?)
        } else {
            warn!("list schemas status: {:?}", status);
            let err = response.json::<ErrorResponse>().await?;
            if status.is_client_error() {
                Err(DeltaSharingError::client(err.to_string()))
            } else {
                Err(DeltaSharingError::server(err.to_string()))
            }
        }
    }

    pub async fn list_schemas(&self, share: &Share) -> Result<Vec<Schema>, DeltaSharingError> {
        let mut schemas = vec![];
        let mut pagination = Pagination::default();
        loop {
            let response = self.list_schemas_paginated(share, &pagination).await?;
            pagination.set_next_token(response.next_page_token().map(ToOwned::to_owned));
            schemas.extend(response);
            if pagination.is_finished() {
                break;
            }
        }
        Ok(schemas)
    }

    pub async fn list_tables_in_schema_paginated(
        &self,
        schema: &Schema,
        pagination: &Pagination,
    ) -> Result<ListTablesResponse, DeltaSharingError> {
        let mut url = url_for_schema(self.profile.endpoint().clone(), schema, Some("tables"));
        url.set_pagination(pagination);
        trace!("URL: {}", url);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();

        if status.is_success() {
            info!("list tables in schema status: {:?}", status);
            Ok(response.json::<ListTablesResponse>().await?)
        } else {
            warn!("list tables in schema status: {:?}", status);
            let err = response.json::<ErrorResponse>().await?;
            if status.is_client_error() {
                Err(DeltaSharingError::client(err.to_string()))
            } else {
                Err(DeltaSharingError::server(err.to_string()))
            }
        }
    }

    pub async fn list_tables(&self, schema: &Schema) -> Result<Vec<Table>, DeltaSharingError> {
        let mut tables = vec![];
        let mut pagination = Pagination::default();
        loop {
            let response = self
                .list_tables_in_schema_paginated(schema, &pagination)
                .await?;
            pagination.set_next_token(response.next_page_token().map(ToOwned::to_owned));
            tables.extend(response);
            if pagination.is_finished() {
                break;
            }
        }
        Ok(tables)
    }

    async fn list_tables_in_share_paginated(
        &self,
        share: &Share,
        pagination: &Pagination,
    ) -> Result<ListTablesResponse, DeltaSharingError> {
        let mut url = url_for_share(self.profile.endpoint().clone(), share, Some("all-tables"));
        url.set_pagination(pagination);
        trace!("URL: {}", url);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();

        if status.is_success() {
            info!("list tables in share status: {:?}", status);
            Ok(response.json::<ListTablesResponse>().await?)
        } else {
            warn!("list tables in share status: {:?}", status);
            let err = response.json::<ErrorResponse>().await?;
            if status.is_client_error() {
                Err(DeltaSharingError::client(err.to_string()))
            } else {
                Err(DeltaSharingError::server(err.to_string()))
            }
        }
    }

    pub async fn list_all_tables(&self, share: &Share) -> Result<Vec<Table>, DeltaSharingError> {
        let mut tables = vec![];
        let mut pagination = Pagination::default();
        loop {
            let response = self
                .list_tables_in_share_paginated(share, &pagination)
                .await?;
            pagination.set_next_token(response.next_page_token().map(ToOwned::to_owned));
            tables.extend(response);
            if pagination.is_finished() {
                break;
            }
        }
        Ok(tables)
    }

    pub async fn get_table_version(
        &self,
        table: &Table,
        starting_timestamp: Option<DateTime<Utc>>,
    ) -> Result<u64, DeltaSharingError> {
        let mut url = url_for_table(self.profile.endpoint().clone(), table, Some("version"));
        if let Some(ts) = starting_timestamp {
            url.query_pairs_mut().append_pair(
                QUERY_PARAM_VERSION_TIMESTAMP,
                &ts.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            );
        }
        trace!("URL: {}", url);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();

        if status.is_success() {
            info!("get table version status: {:?}", status);
            parse_table_version(&response)
        } else {
            warn!("get table version status: {:?}", status);
            let err = response.json::<ErrorResponse>().await?;
            if status.is_client_error() {
                Err(DeltaSharingError::client(err.to_string()))
            } else {
                Err(DeltaSharingError::server(err.to_string()))
            }
        }
    }

    pub async fn get_table_metadata(
        &self,
        table: &Table,
    ) -> Result<(Protocol, Metadata), DeltaSharingError> {
        let url = url_for_table(self.profile.endpoint().clone(), table, Some("metadata"));
        trace!("requesting: {}", url);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();

        if !status.is_success() {
            warn!("get table metadata status: {:?}", status);
            let res = response.json::<ErrorResponse>().await.unwrap();
            if status.is_client_error() {
                return Err(DeltaSharingError::client(res.to_string()));
            } else {
                return Err(DeltaSharingError::server(res.to_string()));
            }
        } else {
            info!("get table metadata status: {:?}", status);
            let full = response.bytes().await?;
            let mut lines = Deserializer::from_slice(&full).into_iter::<ParquetResponse>();

            let protocol = lines
                .next()
                .and_then(Result::ok)
                .and_then(ParquetResponse::to_protocol)
                .ok_or(DeltaSharingError::parse_response("parsing protocol failed"))?;
            let metadata = lines
                .next()
                .and_then(Result::ok)
                .and_then(ParquetResponse::to_metadata)
                .ok_or(DeltaSharingError::parse_response("parsing metadata failed"))?;

            Ok((protocol, metadata))
        }
    }

    pub async fn get_table_data(
        &self,
        table: &Table,
        predicates: Option<String>,
        limit: Option<u32>,
    ) -> Result<Vec<File>, DeltaSharingError> {
        let url = url_for_table(self.profile.endpoint().clone(), table, Some("query"));
        trace!("requesting: {}", url);

        let mut body: HashMap<String, String> = HashMap::new();
        if let Some(pred) = predicates {
            body.insert("jsonPredicateHints".to_string(), pred);
        }
        if let Some(lim) = limit {
            body.insert("limitHint".to_string(), lim.to_string());
        }
        debug!("body: {:?}", body);

        let response = self
            .client
            .request(Method::POST, url)
            .json(&body)
            .authorize_with_profile(&self.profile)?
            .send()
            .await?;
        let status = response.status();

        if !status.is_success() {
            warn!("get table data status: {:?}", status);
            let err = response.json::<ErrorResponse>().await.unwrap();
            if status.is_client_error() {
                return Err(DeltaSharingError::client(err.to_string()));
            } else {
                return Err(DeltaSharingError::server(err.to_string()));
            }
        } else {
            let full = response.bytes().await?;
            let mut lines = Deserializer::from_slice(&full).into_iter::<ParquetResponse>();

            let _ = lines
                .next()
                .and_then(Result::ok)
                .and_then(ParquetResponse::to_protocol)
                .ok_or(DeltaSharingError::parse_response("parsing protocol failed"))?;
            let _ = lines
                .next()
                .and_then(Result::ok)
                .and_then(ParquetResponse::to_metadata)
                .ok_or(DeltaSharingError::parse_response("parsing metadata failed"))?;

            let mut files = vec![];
            while let Some(line) = lines.next() {
                let file = line.ok().and_then(ParquetResponse::to_file);
                if let Some(f) = file {
                    files.push(f);
                }
            }

            Ok(files)
        }
    }

    pub async fn get_table_changes(&self, _table: &Table) {
        todo!()
    }

    async fn request(&self, method: Method, url: Url) -> Result<Response, DeltaSharingError> {
        self.client
            .request(method, url)
            .authorize_with_profile(&self.profile)
            .unwrap()
            .send()
            .await
            .map_err(Into::into)
    }
}

fn url_for_share(endpoint: Url, share: &Share, res: Option<&str>) -> Url {
    let mut url = endpoint;
    url.path_segments_mut()
        .expect("valid base")
        .pop_if_empty()
        .push("shares")
        .push(share.name());
    if let Some(r) = res {
        url.path_segments_mut().expect("valid base").push(r);
    }
    url
}

fn url_for_schema(endpoint: Url, schema: &Schema, res: Option<&str>) -> Url {
    let mut url = endpoint;
    url.path_segments_mut()
        .expect("valid base")
        .pop_if_empty()
        .extend(&["shares", schema.share_name(), "schemas", schema.name()]);
    if let Some(r) = res {
        url.path_segments_mut().expect("valid base").push(r);
    }
    url
}

fn url_for_table(endpoint: Url, table: &Table, res: Option<&str>) -> Url {
    let mut url = endpoint;
    url.path_segments_mut()
        .expect("valid base")
        .pop_if_empty()
        .extend(&[
            "shares",
            table.share_name(),
            "schemas",
            table.schema_name(),
            "tables",
            table.name(),
        ]);
    if let Some(r) = res {
        url.path_segments_mut().expect("valid base").push(r);
    }
    url
}

fn parse_table_version(response: &Response) -> Result<u64, DeltaSharingError> {
    response
        .headers()
        .get("Delta-Table-Version")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or(DeltaSharingError::parse_response("parsing version failed"))
}

#[cfg(test)]
mod test {
    use chrono::{TimeZone, Utc};
    use httpmock::MockServer;
    use serde_json::json;
    use tracing_test::traced_test;

    use crate::profile::ProfileType;

    use super::*;

    #[test]
    fn test_url_for_share() {
        let endpoint = Url::parse("https://example.com/prefix").unwrap();
        let share = Share::new("my-share", None);
        let url = url_for_share(endpoint.clone(), &share, None);
        assert_eq!(url.as_str(), "https://example.com/prefix/shares/my-share");

        let url = url_for_share(endpoint.clone(), &share, Some("res"));
        assert_eq!(
            url.as_str(),
            "https://example.com/prefix/shares/my-share/res"
        );
    }

    #[test]
    fn test_url_for_schema() {
        let endpoint = Url::parse("https://example.com/prefix/").unwrap();
        let schema = Schema::new("my-share", "my-schema");
        let url = url_for_schema(endpoint.clone(), &schema, None);
        assert_eq!(
            url.as_str(),
            "https://example.com/prefix/shares/my-share/schemas/my-schema"
        );

        let url = url_for_schema(endpoint.clone(), &schema, Some("res"));
        assert_eq!(
            url.as_str(),
            "https://example.com/prefix/shares/my-share/schemas/my-schema/res"
        );
    }

    #[test]
    fn test_url_for_table() {
        let endpoint = Url::parse("https://example.com/prefix").unwrap();
        let table = Table::new("my-share", "my-schema", "my-table", None, None);
        let url = url_for_table(endpoint.clone(), &table, None);
        assert_eq!(
            url.as_str(),
            "https://example.com/prefix/shares/my-share/schemas/my-schema/tables/my-table"
        );

        let url = url_for_table(endpoint.clone(), &table, Some("res"));
        assert_eq!(
            url.as_str(),
            "https://example.com/prefix/shares/my-share/schemas/my-schema/tables/my-table/res"
        );
    }

    fn build_sharing_client(server: &MockServer) -> DeltaSharingClient {
        let profile_type = ProfileType::new_bearer_token("test-token", None);
        let mock_server_url = server.base_url().parse::<Url>().unwrap();
        let profile: Profile = Profile::from_profile_type(1, mock_server_url, profile_type);
        DeltaSharingClient::new(profile)
    }

    #[traced_test]
    #[tokio::test]
    async fn list_shares_paginated() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares")
                .query_param("maxResults", "1")
                .query_param("pageToken", "foo")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .body_from_file("./src/client/resources/list_shares.json");
        });
        let client = build_sharing_client(&server);

        let result = client
            .list_shares_paginated(&Pagination::start(Some(1), Some("foo".into())))
            .await
            .unwrap();

        mock.assert();
        assert_eq!(
            result.items(),
            vec![
                Share::new(
                    "vaccine_share",
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f")
                ),
                Share::new("sales_share", Some("3e979c79-6399-4dac-bcf8-54e268f48515"))
            ]
        );
        assert_eq!(result.next_page_token(), Some("..."));
    }

    #[traced_test]
    #[tokio::test]
    async fn get_share() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares/vaccine_share")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .body_from_file("./src/client/resources/get_share.json");
        });
        let client = build_sharing_client(&server);
        let share = Share::new(
            "vaccine_share",
            Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f"),
        );

        let result = client.get_share(&share).await.unwrap();

        mock.assert();
        assert_eq!(result, share);
    }

    #[traced_test]
    #[tokio::test]
    async fn list_schemas_paginated() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares/vaccine_share/schemas")
                .query_param("maxResults", "1")
                .query_param("pageToken", "foo")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .body_from_file("./src/client/resources/list_schemas.json");
        });
        let client = build_sharing_client(&server);
        let share = Share::new("vaccine_share", None);

        let result = client
            .list_schemas_paginated(&share, &Pagination::start(Some(1), Some("foo".into())))
            .await
            .unwrap();

        mock.assert();
        assert_eq!(
            result.items(),
            vec![Schema::new("vaccine_share", "acme_vaccine_data")]
        );
        assert_eq!(result.next_page_token(), Some("..."));
    }

    #[traced_test]
    #[tokio::test]
    async fn list_tables_in_schema_paginated() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares/vaccine_share/schemas/acme_vaccine_data/tables")
                .query_param("maxResults", "1")
                .query_param("pageToken", "foo")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .body_from_file("./src/client/resources/list_tables_in_schema.json");
        });
        let client = build_sharing_client(&server);
        let schema = Schema::new("vaccine_share", "acme_vaccine_data");

        let result = client
            .list_tables_in_schema_paginated(
                &schema,
                &Pagination::start(Some(1), Some("foo".into())),
            )
            .await
            .unwrap();

        mock.assert();
        assert_eq!(
            result.items(),
            vec![
                Table::new(
                    "vaccine_share",
                    "acme_vaccine_data",
                    "vaccine_ingredients",
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".into()),
                    Some("dcb1e680-7da4-4041-9be8-88aff508d001".into())
                ),
                Table::new(
                    "vaccine_share",
                    "acme_vaccine_data",
                    "vaccine_patients",
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".into()),
                    Some("c48f3e19-2c29-4ea3-b6f7-3899e53338fa".into())
                )
            ]
        );
        assert_eq!(result.next_page_token(), Some("..."));
    }

    #[traced_test]
    #[tokio::test]
    async fn list_tables_in_share_paginated() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares/vaccine_share/all-tables")
                .query_param("maxResults", "1")
                .query_param("pageToken", "foo")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .body_from_file("./src/client/resources/list_tables_in_share.json");
        });
        let client = build_sharing_client(&server);
        let share = Share::new("vaccine_share", None);

        let result = client
            .list_tables_in_share_paginated(&share, &Pagination::start(Some(1), Some("foo".into())))
            .await
            .unwrap();

        mock.assert();
        assert_eq!(
            result.items(),
            vec![
                Table::new(
                    "vaccine_share",
                    "acme_vaccine_ingredient_data",
                    "vaccine_ingredients",
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".into()),
                    Some("2f9729e9-6fcf-4d34-96df-bf72b26dfbe9".into())
                ),
                Table::new(
                    "vaccine_share",
                    "acme_vaccine_patient_data",
                    "vaccine_patients",
                    Some("edacc4a7-6600-4fbb-85f3-a62a5ce6761f".into()),
                    Some("74be6365-0fc8-4a2f-8720-0de125bb5832".into())
                )
            ]
        );
        assert_eq!(result.next_page_token(), Some("..."));
    }

    #[traced_test]
    #[tokio::test]
    async fn get_table_version() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/version")
                .query_param("startingTimestamp", "2022-01-01T00:00:00Z")
                .header_exists("authorization");
            then.status(200)
                .header("delta-table-version", "123")
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .body("");
        });
        let client = build_sharing_client(&server);
        let table = Table::new(
            "vaccine_share",
            "acme_vaccine_data",
            "vaccine_patients",
            None,
            None,
        );
        let starting_timestamp = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();

        let result = client
            .get_table_version(&table, Some(starting_timestamp))
            .await
            .unwrap();

        mock.assert();
        assert_eq!(result, 123);
    }

    #[traced_test]
    #[tokio::test]
    async fn get_table_metadata() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("GET")
                .path("/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/metadata")
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/x-ndjson")
                .header("charset", "utf-8")
                .header("delta-table-version", "123")
                .body_from_file("./src/client/resources/get_table_metadata.ndjson");
        });
        let client = build_sharing_client(&server);
        let table = Table::new(
            "vaccine_share",
            "acme_vaccine_data",
            "vaccine_patients",
            None,
            None,
        );

        let (protocol, metadata) = client.get_table_metadata(&table).await.unwrap();

        mock.assert();
        assert_eq!(protocol.min_reader_version(), 1);
        assert_eq!(metadata.id(), "table_id");
        assert_eq!(metadata.format().provider(), "parquet");
        assert_eq!(metadata.schema_string(), "schema_as_string");
        assert_eq!(metadata.partition_columns(), &["date"]);
    }

    #[traced_test]
    #[tokio::test]
    async fn get_table_data() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("POST")
                .path(
                    "/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/query",
                )
                .json_body(json!({
                    "jsonPredicateHints": "{\"foo\": \"bar\"}",
                    "limitHint": "100"
                }))
                .header_exists("authorization");
            then.status(200)
                .header("content-type", "application/x-ndjson")
                .header("charset", "utf-8")
                .header("delta-table-version", "123")
                .body_from_file("./src/client/resources/get_table_data.ndjson");
        });
        let client = build_sharing_client(&server);
        let table = Table::new(
            "vaccine_share",
            "acme_vaccine_data",
            "vaccine_patients",
            None,
            None,
        );

        let result = client
            .get_table_data(&table, Some("{\"foo\": \"bar\"}".into()), Some(100))
            .await
            .unwrap();

        mock.assert();
        assert_eq!(result.len(), 2);
    }

    #[traced_test]
    #[tokio::test]
    async fn get_table_data_not_found() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("POST")
                .path("/shares/my_share/schemas/my_schema/tables/fake_table/query");
            then.status(404)
                .header("content-type", "application/json")
                .header("charset", "utf-8")
                .body(r#"{"errorCode": "RESOURCE_DOES_NOT_EXIST", "message": "[Share/Schema/Table] 'my_share/my_schema/fake_table' does not exist, please contact your share provider for further information."}"#);
        });
        let client = build_sharing_client(&server);
        let table = "my_share.my_schema.fake_table".parse::<Table>().unwrap();

        let result = client.get_table_data(&table, None, None).await;

        mock.assert();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().starts_with("[SHARING_CLIENT_ERROR] [RESOURCE_DOES_NOT_EXIST] [Share/Schema/Table] 'my_share/my_schema/fake_table' does not exist"));
    }
}
