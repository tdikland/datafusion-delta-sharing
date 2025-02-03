//! Delta Sharing client
//!
//! This module contains the high level client for interacting with the Delta Sharing server. The
//! client is responsible for making requests to the server and handling the responses. It covers
//! all APIs that are specified by the Delta Sharing protocol.
//!
//! # Example
//! ```no_run
//! use delta_sharing::{Client, Profile, ShareName};
//! use futures::StreamExt;
//!
//! async fn main() -> Result<(), delta_sharing::ClientError> {
//!     let profile = Profile::try_from_path("path/to/profile.share")?;
//!     let client = Client::new(profile);
//!
//!     let shares = client.list_shares().await.collect::<Vec<_>>().await?;
//! }
//! ```

use std::{fmt, path::Path};

use futures::Stream;
use request::{QueryTableVersion, QueryTableVersionOpts, TableVersion, TableVersionRange};
use response::{TableData, TableDataFormat, TableMetadataFormat};
use tracing::instrument;

use crate::model::{SchemaInfo, ShareInfo, TableInfo, TableVersionNumber};

mod error;
pub mod expr;
mod pagination;
pub mod profile;
mod request;
mod response;
mod rest;

use pagination::{Page, Paginated};
use rest::{
    response::{MetadataResponseLines, TableDataResponseLines},
    RestClient,
};

pub use error::ClientError;
pub use profile::Profile;
pub use request::{ParseNameError, QueryTableDataOpts, SchemaName, ShareName, TableName};
pub use response::TableMetadata;

/// High level Delta Sharing client.
#[derive(Clone)]
pub struct Client {
    inner: RestClient,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish()
    }
}

impl Client {
    /// Create a new client using the profile at the specified path.
    pub fn try_from_path<P: AsRef<Path>>(path: P) -> Result<Self, ClientError> {
        let profile = Profile::try_from_path(path)?;
        Ok(Self::new(profile))
    }

    /// Create a new client with the given profile.
    pub fn new(profile: Profile) -> Self {
        Self {
            inner: RestClient::new(profile),
        }
    }

    /// Get the profile associated with this client.
    pub fn profile(&self) -> &Profile {
        self.inner.profile()
    }
}

impl Client {
    /// List all available shares.
    #[instrument(level = "debug", skip(self))]
    pub async fn list_shares(&self) -> impl Stream<Item = Result<ShareInfo, ClientError>> {
        let client = self.inner.clone();
        Paginated::new(move |pagination| {
            let client = client.clone();
            async move {
                let req = rest::request::ListSharesRequest::builder()
                    .maybe_max_results(pagination.max_results())
                    .maybe_page_token(pagination.page_token())
                    .build();
                client
                    .send(req)
                    .await
                    .map(|res| Page::new(res.items, res.next_page_token))
                    .map_err(Into::into)
            }
        })
    }

    /// Get a share by name.
    #[instrument(level = "debug", skip(self))]
    pub async fn get_share(&self, share: ShareName) -> Result<Option<ShareInfo>, ClientError> {
        let req = rest::request::GetShareRequest::builder()
            .share(share.name())
            .build();
        match self.inner.send(req).await {
            Ok(res) => Ok(Some(res.share)),
            Err(err) if err.is_not_found() => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// List all available schemas.
    #[instrument(level = "debug", skip(self))]
    pub async fn list_schemas(
        &self,
        share: ShareName,
    ) -> impl Stream<Item = Result<SchemaInfo, ClientError>> {
        let client = self.inner.clone();
        Paginated::new(move |pagination| {
            let client = client.clone();
            let share_name = share.name().to_owned();
            async move {
                let request = rest::request::ListSchemasRequest::builder()
                    .maybe_max_results(pagination.max_results())
                    .maybe_page_token(pagination.page_token())
                    .share(&share_name)
                    .build();
                client
                    .send(request)
                    .await
                    .map(|res| Page::new(res.items, res.next_page_token))
                    .map_err(Into::into)
            }
        })
    }

    /// List all available tables in a share.
    #[instrument(level = "debug", skip(self))]
    pub async fn list_tables_in_share(
        &self,
        share: ShareName,
    ) -> impl Stream<Item = Result<TableInfo, ClientError>> {
        let client = self.inner.clone();
        Paginated::new(move |pagination| {
            let client = client.clone();
            let share_name = share.name().to_owned();
            async move {
                let req = rest::request::ListTablesInShareRequest::builder()
                    .maybe_max_results(pagination.max_results())
                    .maybe_page_token(pagination.page_token())
                    .share(&share_name)
                    .build();
                client
                    .send(req)
                    .await
                    .map(|res| Page::new(res.items, res.next_page_token))
                    .map_err(Into::into)
            }
        })
    }

    /// List all available tables in a schema.
    #[instrument(level = "debug", skip(self))]
    pub async fn list_tables_in_schema(
        &self,
        schema: SchemaName,
    ) -> impl Stream<Item = Result<TableInfo, ClientError>> {
        let client = self.inner.clone();
        Paginated::new(move |p| {
            let client = client.clone();
            let schema = schema.clone();
            async move {
                let request = rest::request::ListTablesInSchemaRequest::builder()
                    .share(schema.share())
                    .schema(schema.name())
                    .maybe_max_results(p.max_results())
                    .maybe_page_token(p.page_token())
                    .build();
                let response = client.send(request).await;
                match response {
                    Ok(s) => Ok(Page::new(s.items, s.next_page_token)),
                    Err(e) => Err(e.into()),
                }
            }
        })
    }

    /// Query the table version number.
    #[instrument(level = "debug", skip(self))]
    pub async fn query_table_version(
        &self,
        table: TableName,
        opts: &QueryTableVersionOpts,
    ) -> Result<TableVersionNumber, ClientError> {
        let ts_formatted = opts.timestamp().map(|ts| ts.to_rfc3339());
        let request = rest::request::QueryTableVersionRequest::builder()
            .share(table.share())
            .schema(table.schema())
            .table(table.name())
            .maybe_starting_timestamp(ts_formatted.as_deref())
            .build();

        let response = self.inner.send(request).await?;
        Ok(response.version)
    }

    /// Query the table metadata.
    #[instrument(level = "debug", skip(self))]
    pub async fn query_table_metadata(
        &self,
        table: TableName,
    ) -> Result<TableMetadata, ClientError> {
        let request = rest::request::QueryTableMetadataRequest::builder()
            .share(table.share())
            .schema(table.schema())
            .table(table.name())
            .build();

        let response = self.inner.send(request).await?;
        match response.lines {
            MetadataResponseLines::Parquet(lines) => {
                let mut lines = lines.into_iter();
                let protocol = lines
                    .next()
                    .expect("no probs")
                    .to_protocol()
                    .expect("no probs");
                let metadata = lines
                    .next()
                    .expect("no probs")
                    .to_metadata()
                    .expect("no probs");
                Ok(TableMetadata::new(
                    response.version,
                    TableMetadataFormat::Parquet { protocol, metadata },
                ))
            }
            _ => panic!(),
        }
    }

    /// Query the table metadata and data.
    #[instrument(level = "debug", skip(self))]
    pub async fn query_table_data(
        &self,
        table: &TableName,
        options: QueryTableDataOpts,
    ) -> Result<TableData, ClientError> {
        let partial_request = rest::request::QueryTableDataRequest::builder()
            .share(table.share())
            .schema(table.schema())
            .table(table.name())
            .maybe_json_predicate_hints(
                options
                    .predicate()
                    .map(|op| serde_json::to_string(&op).expect("predicate to be serializable")),
            )
            .maybe_limit_hint(options.limit().map(|limit| limit as i32));

        let request = if let Some(v) = options.version() {
            match v {
                QueryTableVersion::PointInTime(table_version_point) => match table_version_point {
                    TableVersion::Number(version) => {
                        partial_request.version(*version as i64).build()
                    }
                    TableVersion::Timestamp(timestamp) => {
                        partial_request.timestamp(timestamp.to_rfc3339()).build()
                    }
                },
                QueryTableVersion::Range(table_version_range) => match table_version_range {
                    TableVersionRange::Version { start, end } => {
                        if let Some(end) = end {
                            partial_request
                                .starting_version(*start as i64)
                                .ending_version(*end as i64)
                                .build()
                        } else {
                            partial_request.starting_version(*start as i64).build()
                        }
                    }
                    TableVersionRange::Timestamp { .. } => unreachable!(),
                },
            }
        } else {
            partial_request.build()
        };

        let response = self.inner.send(request).await?;

        let data = match response.lines {
            TableDataResponseLines::Parquet(d) => TableData {
                version: response.version,
                format: TableDataFormat::Parquet {
                    protocol: d.protocol,
                    metadata: d.metadata,
                    files: d.files,
                },
            },
        };
        Ok(data)
    }

    /// Query the table changes.
    pub async fn query_table_changes(&self) -> Result<(), ClientError> {
        let _req = rest::request::QueryTableChangesRequest::builder()
            .share("share")
            .schema("schema")
            .table("table")
            .build();
        todo!()
    }
}
