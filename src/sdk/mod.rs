use chrono::{DateTime, Utc};
use error::ClientError;
use futures::Stream;

use crate::{
    auth::Profile,
    expr::Op,
    model::{
        action::parquet::{Metadata, Protocol},
        Schema, Share, Table, TableVersion,
    },
    rest::{
        self,
        request::{self, GetShareRequest},
        response::MetadataResponseLines,
        RestClient,
    },
};

mod error;
mod pagination;

struct Pagination {
    max_results: Option<u32>,
    page_token: Option<String>,
    done: bool,
}

impl Pagination {
    fn begin() -> Self {
        Self {
            max_results: None,
            page_token: None,
            done: false,
        }
    }
}

pub struct Client {
    inner: rest::RestClient,
}

impl Client {
    pub fn new(profile: Profile) -> Self {
        Self {
            inner: RestClient::new(profile),
        }
    }

    pub async fn list_shares(&self) -> impl Stream<Item = Result<Share, ClientError>> {
        let mut shares: Vec<Result<Share, ClientError>> = vec![];
        let mut pagination = Pagination::begin();

        while !pagination.done {
            let request = request::ListSharesRequest::builder()
                .maybe_max_results(pagination.max_results.map(|m| m.try_into().unwrap()))
                .maybe_page_token(pagination.page_token.clone())
                .build();

            let response = self.inner.send(request).await;
            match response {
                Ok(s) => {
                    shares.extend(s.items.into_iter().map(Ok));
                    pagination.page_token = s.next_page_token;
                    pagination.done = pagination.page_token.is_none();
                }
                Err(e) => {
                    return futures::stream::iter(vec![Err(e.into())]);
                }
            }
        }

        futures::stream::iter(shares)
    }

    pub async fn get_share(
        &self,
        share_name: impl Into<String>,
    ) -> Result<Option<Share>, ClientError> {
        let request = GetShareRequest::builder()
            .share_name(share_name.into())
            .build();
        let response = self.inner.send(request).await;
        match response {
            Ok(r) => Ok(Some(r.share)),
            Err(e) if e.is_not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn list_schemas(
        &self,
        share_name: impl Into<String>,
    ) -> impl Stream<Item = Result<Schema, ClientError>> {
        let mut schemas: Vec<Result<Schema, ClientError>> = vec![];
        let mut pagination = Pagination::begin();
        let share_name = share_name.into();

        while !pagination.done {
            let request = request::ListSchemasRequest::builder()
                .share_name(share_name.clone())
                .maybe_max_results(pagination.max_results.map(|m| m.try_into().unwrap()))
                .maybe_page_token(pagination.page_token.clone())
                .build();

            let response = self.inner.send(request).await;
            match response {
                Ok(s) => {
                    schemas.extend(s.items.into_iter().map(Ok));
                    pagination.page_token = s.next_page_token;
                    pagination.done = pagination.page_token.is_none();
                }
                Err(e) => {
                    return futures::stream::iter(vec![Err(e.into())]);
                }
            }
        }

        futures::stream::iter(schemas)
    }

    pub async fn list_tables_in_share(
        &self,
        share_name: impl Into<String>,
    ) -> impl Stream<Item = Result<Table, ClientError>> {
        let mut tables: Vec<Result<Table, ClientError>> = vec![];
        let mut pagination = Pagination::begin();
        let share_name = share_name.into();

        while !pagination.done {
            let request = request::ListTablesInShareRequest::builder()
                .share_name(share_name.clone())
                .maybe_max_results(pagination.max_results.map(|m| m.try_into().unwrap()))
                .maybe_page_token(pagination.page_token.clone())
                .build();

            let response = self.inner.send(request).await;
            match response {
                Ok(s) => {
                    tables.extend(s.items.into_iter().map(Ok));
                    pagination.page_token = s.next_page_token;
                    pagination.done = pagination.page_token.is_none();
                }
                Err(e) => {
                    return futures::stream::iter(vec![Err(e.into())]);
                }
            }
        }

        futures::stream::iter(tables)
    }

    pub async fn list_tables_in_schema(
        &self,
        share_name: impl Into<String>,
        schema_name: impl Into<String>,
    ) -> impl Stream<Item = Result<Table, ClientError>> {
        let mut tables: Vec<Result<Table, ClientError>> = vec![];
        let mut pagination = Pagination::begin();
        let share_name = share_name.into();
        let schema_name = schema_name.into();

        while !pagination.done {
            let request = request::ListTablesInSchemaRequest::builder()
                .share_name(share_name.clone())
                .schema_name(schema_name.clone())
                .maybe_max_results(pagination.max_results.map(|m| m.try_into().unwrap()))
                .maybe_page_token(pagination.page_token.clone())
                .build();

            let response = self.inner.send(request).await;
            match response {
                Ok(s) => {
                    tables.extend(s.items.into_iter().map(Ok));
                    pagination.page_token = s.next_page_token;
                    pagination.done = pagination.page_token.is_none();
                }
                Err(e) => {
                    return futures::stream::iter(vec![Err(e.into())]);
                }
            }
        }

        futures::stream::iter(tables)
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

    pub async fn query_table_metadata(
        &self,
        share_name: impl Into<String>,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<(), ClientError> {
        let request = request::QueryTableMetadataRequest::builder()
            .share_name(share_name.into())
            .schema_name(schema_name.into())
            .table_name(table_name.into())
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

    pub async fn query_table_data(
        &self,
        share_name: impl Into<String>,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
        options: QueryTableDataOpts,
    ) -> Result<(), ClientError> {
        // let mut partial_request = request::QueryTableDataRequest::builder()
        //     .share_name(share_name.into())
        //     .schema_name(schema_name.into())
        //     .table_name(table_name.into())
        //     .maybe_json_predicate_hints(
        //         options
        //             .predicate
        //             .map(|op| serde_json::to_string(&op).unwrap()),
        //     )
        //     .maybe_limit_hint(options.limit.map(|limit| limit as i32));

        // let request = if let Some(v) = options.version {
        //     match v {
        //         QueryTableVersion::PointInTime(table_version_point) => match table_version_point {
        //             TableVersionPoint::Number(version) => {
        //                 partial_request.version(version as i64).build()
        //             }
        //             TableVersionPoint::Timestamp(timestamp) => {
        //                 partial_request.timestamp(timestamp.to_rfc3339()).build()
        //             }
        //         },
        //         QueryTableVersion::Range(table_version_range) => match table_version_range {
        //             TableVersionRange::Version { start, end } => {
        //                 if let Some(end) = end {
        //                     partial_request
        //                         .starting_version(start as i64)
        //                         .ending_version(end as i64)
        //                         .build()
        //                 } else {
        //                     partial_request.starting_version(start as i64).build()
        //                 }
        //             }
        //             TableVersionRange::Timestamp { .. } => unreachable!(),
        //         },
        //     }
        // } else {
        //     partial_request.build()
        // };

        // let response = self.inner.send(request).await?;

        todo!()
    }
}

#[derive(Debug)]
pub struct TableMetadata {
    pub version: TableVersion,
    pub format: TableMetadataFormat,
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
