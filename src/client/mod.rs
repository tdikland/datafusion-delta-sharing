use reqwest::{Client, Method, Response, StatusCode, Url};
use serde::Deserialize;
use serde_json::{json, Deserializer};
use tracing::{debug, info, trace, warn};

use self::{
    action::File,
    pagination::{Pagination, PaginationExt},
};

use crate::{
    error::DeltaSharingError,
    securable::{Schema, Share, Table},
};
use {
    action::{Metadata, Protocol},
    profile::DeltaSharingProfile,
    response::{
        ErrorResponse, ListSchemasPaginated, ListSharesPaginated, ListTablesPaginated,
        ParquetResponse,
    },
};

pub mod action;
pub mod pagination;
pub mod profile;
pub mod response;

const QUERY_PARAM_VERSION_TIMESTAMP: &'static str = "startingTimestamp";

#[derive(Debug, Clone)]
pub struct DeltaSharingClient {
    client: Client,
    profile: DeltaSharingProfile,
    endpoint: Url,
}

impl DeltaSharingClient {
    pub fn new(profile: DeltaSharingProfile) -> Self {
        Self {
            client: Client::new(),
            profile: profile.clone(),
            endpoint: profile.url(),
        }
    }

    pub fn profile(&self) -> &DeltaSharingProfile {
        &self.profile
    }

    pub async fn list_shares_paginated(
        &self,
        pagination: &Pagination,
    ) -> Result<ListSharesPaginated, DeltaSharingError> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .expect("valid base")
            .push(&format!("/shares"));
        url.set_pagination(&pagination);

        debug!("requesting: {}", url);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();

        info!("STATUS: {}", status);

        response
            .json::<ServerResponse<ListSharesPaginated>>()
            .await?
            .with_status(status)
    }

    pub async fn list_shares(&self) -> Result<Vec<Share>, DeltaSharingError> {
        let mut shares = vec![];
        let mut pagination = Pagination::default();
        loop {
            trace!("requesting page of shares: {:?}", pagination);
            let response = self.list_shares_paginated(&pagination).await?;
            pagination.set_next_token(response.next_page_token().map(ToOwned::to_owned));
            shares.extend(response);
            if pagination.is_finished() {
                break;
            }
        }
        Ok(shares)
    }

    pub async fn list_schemas_paginated(
        &self,
        share: &Share,
        pagination: &Pagination,
    ) -> Result<ListSchemasPaginated, DeltaSharingError> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .expect("valid base")
            .push(&format!("/shares/{}/schemas", share));
        url.set_pagination(pagination);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();
        response
            .json::<ServerResponse<ListSchemasPaginated>>()
            .await?
            .with_status(status)
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
    ) -> Result<ListTablesPaginated, DeltaSharingError> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut().expect("valid base").push(&format!(
            "/shares/{}/schemas/{}/tables",
            schema.share_name(),
            schema.name()
        ));
        url.set_pagination(pagination);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();
        response
            .json::<ServerResponse<ListTablesPaginated>>()
            .await?
            .with_status(status)
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
    ) -> Result<ListTablesPaginated, DeltaSharingError> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .expect("valid base")
            .push(&format!("/shares/{}/all-tables", share.name()));
        url.set_pagination(pagination);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();
        response
            .json::<ServerResponse<ListTablesPaginated>>()
            .await?
            .with_status(status)
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
        starting_timestamp: Option<&str>,
    ) -> Result<u64, DeltaSharingError> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut().expect("valid base").push(&format!(
            "/shares/{}/schemas/{}/tables/{}/version",
            table.share_name(),
            table.schema_name(),
            table.name()
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
            .ok_or(DeltaSharingError::parse_response("parsing version failed"))
    }

    pub async fn get_table_metadata(
        &self,
        table: &Table,
    ) -> Result<(Protocol, Metadata), DeltaSharingError> {
        let mut url = self.endpoint.clone();
        url.set_path(&format!(
            "{}/shares/{}/schemas/{}/tables/{}/metadata",
            url.path(),
            table.share_name(),
            table.schema_name(),
            table.name()
        ));
        debug!("requesting: {}", url);

        let response = self.request(Method::GET, url).await?;
        let status = response.status();
        debug!("STATUS: {}", status);

        if !status.is_success() {
            let res = response.json::<ErrorResponse>().await.unwrap();
            if status.is_client_error() {
                return Err(DeltaSharingError::client(res.to_string()));
            } else if status.is_server_error() {
                return Err(DeltaSharingError::server(res.to_string()));
            } else {
                return Err(DeltaSharingError::other("unknown error"));
            }
        } else {
            let full = response.bytes().await?;
            let mut lines = Deserializer::from_slice(&full).into_iter::<ParquetResponse>();

            let protocol = lines
                .next()
                .and_then(Result::ok)
                .and_then(ParquetResponse::to_protocol)
                .ok_or(DeltaSharingError::parse_response("parsing protocol failed"))?;
            let metadata = dbg!(lines.next())
                .and_then(Result::ok)
                .and_then(ParquetResponse::to_metadata)
                .ok_or(DeltaSharingError::parse_response("parsing metadata failed"))?;

            Ok((protocol, metadata))
        }
    }

    pub async fn get_table_data(
        &self,
        table: &Table,
        _predicates: Option<String>,
        _limit: Option<u32>,
    ) -> Result<Vec<File>, DeltaSharingError> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut().expect("valid base").extend([
            "shares",
            table.share_name(),
            "schemas",
            table.schema_name(),
            "tables",
            table.name(),
            "query",
        ]);

        debug!("requesting: {}", url);

        let request = self
            .client
            .request(Method::POST, url)
            .json(&json!({}))
            .bearer_auth(self.profile.token());

        debug!("request: {:?}", request);
        let response = request.send().await?;
        let status = response.status();

        if !status.is_success() {
            let res = response.json::<ErrorResponse>().await.unwrap();
            if status.is_client_error() {
                return Err(DeltaSharingError::client(res.to_string()));
            } else if status.is_server_error() {
                return Err(DeltaSharingError::server(res.to_string()));
            } else {
                return Err(DeltaSharingError::other("unknown error"));
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
            .bearer_auth(self.profile.token())
            .send()
            .await
            .map_err(Into::into)
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ServerResponse<T> {
    Error(ErrorResponse),
    Succes(T),
}

impl<T> ServerResponse<T> {
    pub fn with_status(self, status: StatusCode) -> Result<T, DeltaSharingError> {
        match self {
            ServerResponse::Error(e) => {
                if status.is_client_error() {
                    Err(DeltaSharingError::client(e.to_string()))
                } else if status.is_server_error() {
                    Err(DeltaSharingError::server(e.to_string()))
                } else {
                    Err(DeltaSharingError::other("unknown error"))
                }
            }
            ServerResponse::Succes(t) => Ok(t),
        }
    }
}
