use core::fmt;
use std::convert::Infallible;

use chrono::{DateTime, Utc};
use futures::Stream;

use crate::{
    auth::Profile,
    expr::Op,
    model::{
        action::parquet::{File, Metadata, Protocol},
        SchemaInfo, ShareInfo, TableInfo, TableVersion,
    },
    rest::{
        self,
        error::RestClientError,
        request::{self, GetShareRequest},
        response::MetadataResponseLines,
        RestClient,
    },
};

mod error;
mod pagination;

pub use error::ClientError;
use pagination::{Page, Paginated};

#[derive(Clone)]
pub struct Client {
    inner: rest::RestClient,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaSharingClient").finish()
    }
}

impl Client {
    pub fn new(profile: Profile) -> Self {
        Self {
            inner: RestClient::new(profile),
        }
    }

    pub fn profile(&self) -> &Profile {
        self.inner.profile()
    }
}

impl Client {
    pub async fn list_shares(&self) -> impl Stream<Item = Result<ShareInfo, ClientError>> {
        let client = self.inner.clone();
        Paginated::new(move |pagination| {
            let client = client.clone();
            async move {
                let req = request::ListSharesRequest::builder()
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

    pub async fn get_share(&self, share: ShareName) -> Result<Option<ShareInfo>, ClientError> {
        let req = GetShareRequest::builder().share_name(share.name).build();
        match self.inner.send(req).await {
            Ok(res) => Ok(Some(res.share)),
            Err(err) if err.is_not_found() => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn list_schemas(
        &self,
        share: ShareName,
    ) -> impl Stream<Item = Result<SchemaInfo, ClientError>> {
        let client = self.inner.clone();
        Paginated::new(move |pagination| {
            let client = client.clone();
            let share_name = share.name.clone();
            async move {
                let request = request::ListSchemasRequest::builder()
                    .maybe_max_results(pagination.max_results().map(|m| m as i32))
                    .maybe_page_token(pagination.page_token().clone())
                    .share_name(share_name.clone())
                    .build();
                client
                    .send(request)
                    .await
                    .map(|res| Page::new(res.items, res.next_page_token))
                    .map_err(Into::into)
            }
        })
    }

    pub async fn list_tables_in_share(
        &self,
        share: ShareName,
    ) -> impl Stream<Item = Result<TableInfo, ClientError>> {
        let client = self.inner.clone();
        Paginated::new(move |pagination| {
            let client = client.clone();
            let share_name = share.name.clone();
            async move {
                let req = request::ListTablesInShareRequest::builder()
                    .maybe_max_results(pagination.max_results())
                    .maybe_page_token(pagination.page_token())
                    .share_name(share_name)
                    .build();
                client
                    .send(req)
                    .await
                    .map(|res| Page::new(res.items, res.next_page_token))
                    .map_err(Into::into)
            }
        })
    }

    pub async fn list_tables_in_schema(
        &self,
        schema: SchemaName,
    ) -> impl Stream<Item = Result<TableInfo, ClientError>> {
        let client = self.inner.clone();
        Paginated::new(move |p| {
            let client = client.clone();
            let schema = schema.clone();
            async move {
                let request = request::ListTablesInSchemaRequest::builder()
                    .share_name(schema.share_name)
                    .schema_name(schema.schema_name)
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

    pub async fn query_table_version(
        &self,
        share_name: impl Into<String>,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
        opts: &QueryTableVersionOpts,
    ) -> Result<TableVersion, ClientError> {
        let partial_request = request::QueryTableVersionRequest::builder()
            .share_name(share_name.into())
            .schema_name(schema_name.into())
            .table_name(table_name.into());

        let request = if let Some(ts) = opts.timestamp {
            partial_request.starting_timestamp(ts.to_rfc3339()).build()
        } else {
            partial_request.build()
        };

        let response = self.inner.send(request).await?;
        Ok(response.version)
    }

    pub async fn query_table_metadata<T, E>(
        &self,
        table_name: T,
    ) -> Result<TableMetadata, ClientError>
    where
        T: TryInto<TableName, Error = E>,
        ClientError: From<E>,
    {
        let table_name = table_name.try_into()?;

        let request = request::QueryTableMetadataRequest::builder()
            .share_name(table_name.share_name)
            .schema_name(table_name.schema_name)
            .table_name(table_name.table_name)
            .build();

        let response = self.inner.send(request).await?;
        match response.lines {
            MetadataResponseLines::Parquet(p) => {
                todo!()
            }
            _ => panic!(),
        }
        todo!()
    }

    pub async fn query_table_data<T, E>(
        &self,
        table: T,
        options: QueryTableDataOpts,
    ) -> Result<TableData, ClientError>
    where
        T: TryInto<TableName, Error = E>,
        ClientError: From<E>,
    {
        let table = table.try_into()?;

        let mut partial_request = request::QueryTableDataRequest::builder()
            .share_name(table.share_name)
            .schema_name(table.schema_name)
            .table_name(table.table_name)
            .maybe_json_predicate_hints(
                options
                    .predicate
                    .map(|op| serde_json::to_string(&op).unwrap()),
            )
            .maybe_limit_hint(options.limit.map(|limit| limit as i32));

        let request = if let Some(v) = options.version {
            match v {
                QueryTableVersion::PointInTime(table_version_point) => match table_version_point {
                    TableVersionPoint::Number(version) => {
                        partial_request.version(version as i64).build()
                    }
                    TableVersionPoint::Timestamp(timestamp) => {
                        partial_request.timestamp(timestamp.to_rfc3339()).build()
                    }
                },
                QueryTableVersion::Range(table_version_range) => match table_version_range {
                    TableVersionRange::Version { start, end } => {
                        if let Some(end) = end {
                            partial_request
                                .starting_version(start as i64)
                                .ending_version(end as i64)
                                .build()
                        } else {
                            partial_request.starting_version(start as i64).build()
                        }
                    }
                    TableVersionRange::Timestamp { .. } => unreachable!(),
                },
            }
        } else {
            partial_request.build()
        };

        let response = self.inner.send(request).await?;

        todo!()
    }
}

impl From<ParseError> for ClientError {
    fn from(e: ParseError) -> Self {
        match e {
            ParseError::InvalidTableRef => ClientError::InvalidTableRef,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShareName {
    name: String,
}

impl TryFrom<&str> for ShareName {
    type Error = Infallible;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.to_owned(),
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct TableName {
    share_name: String,
    schema_name: String,
    table_name: String,
}

#[derive(Debug, Clone)]
pub struct SchemaName {
    share_name: String,
    schema_name: String,
}

#[derive(Debug)]
pub enum ParseError {
    InvalidTableRef,
}

impl TryFrom<String> for TableName {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() != 3 {
            return Err(ParseError::InvalidTableRef);
        }

        Ok(Self {
            share_name: parts[0].to_string(),
            schema_name: parts[1].to_string(),
            table_name: parts[2].to_string(),
        })
    }
}

impl From<&str> for SchemaName {
    fn from(value: &str) -> Self {
        let parts: Vec<&str> = value.split('.').collect();
        Self {
            share_name: parts[0].to_string(),
            schema_name: parts[1].to_string(),
        }
    }
}

impl TryFrom<&str> for TableName {
    type Error = ParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() != 3 {
            return Err(ParseError::InvalidTableRef);
        }

        Ok(Self {
            share_name: parts[0].to_string(),
            schema_name: parts[1].to_string(),
            table_name: parts[2].to_string(),
        })
    }
}

impl TryFrom<String> for SchemaName {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() != 2 {
            return Err(ParseError::InvalidTableRef);
        }

        Ok(Self {
            share_name: parts[0].to_string(),
            schema_name: parts[1].to_string(),
        })
    }
}

#[derive(Debug)]
pub struct TableData {
    version: TableVersion,
    format: TableDataFormat,
}

impl TableData {
    pub fn into_parquet_files(self) -> Vec<File> {
        match self.format {
            TableDataFormat::Parquet {
                protocol,
                metadata,
                files,
            } => files,
        }
    }
}

#[derive(Debug)]
pub enum TableDataFormat {
    Parquet {
        protocol: Protocol,
        metadata: Metadata,
        files: Vec<File>,
    },
}

#[derive(Debug)]
pub struct TableMetadata {
    version: TableVersion,
    format: TableMetadataFormat,
}

impl TableMetadata {
    pub fn schema_string(&self) -> &str {
        match &self.format {
            TableMetadataFormat::Parquet { metadata, .. } => metadata.schema_string(),
        }
    }
}

#[derive(Debug)]
pub enum TableMetadataFormat {
    Parquet {
        protocol: Protocol,
        metadata: Metadata,
    },
}

#[derive(Debug, Default)]
pub struct QueryTableVersionOpts {
    timestamp: Option<DateTime<Utc>>,
}

#[derive(Default)]
pub struct QueryTableDataOpts {
    predicate: Option<Op>,
    limit: Option<u32>,
    version: Option<QueryTableVersion>,
}

enum TableVersionPoint {
    Number(u64),
    Timestamp(DateTime<Utc>),
}

enum TableVersionRange {
    Version {
        start: u64,
        end: Option<u64>,
    },
    Timestamp {
        start: DateTime<Utc>,
        end: Option<DateTime<Utc>>,
    },
}

enum QueryTableVersion {
    PointInTime(TableVersionPoint),
    Range(TableVersionRange),
}
