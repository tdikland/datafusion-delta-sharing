use async_trait::async_trait;
use reqwest::Response;

use crate::model::TableVersion;

use super::line::{DeltaResponseLine, ParquetResponseLine, ResponseLine};
use super::util::{extract_delta_table_version, has_ndjson_content_type};
use super::{FromResponse, ParseResponseError};

pub struct QueryTableChangesResponse {
    pub version: TableVersion,
    pub changes: TableChangesResponseLines,
}

pub enum TableChangesResponseLines {
    Parquet(Vec<ParquetResponseLine>),
    Delta(Vec<DeltaResponseLine>),
}

#[async_trait]
impl FromResponse for QueryTableChangesResponse {
    type Error = ParseResponseError;

    async fn parse(res: Response) -> Result<Self, Self::Error> {
        if !has_ndjson_content_type(res.headers()) {
            return Err(ParseResponseError::MissingNdJsonContentType);
        }

        let table_version = extract_delta_table_version(res.headers())?;

        let bytes = res
            .bytes()
            .await
            .map_err(|e| ParseResponseError::BodyError {
                source: Box::new(e),
            })?;

        let mut deserializer =
            serde_json::Deserializer::from_slice(&bytes).into_iter::<ResponseLine>();

        let protocol_response_line = deserializer
            .next()
            .and_then(Result::ok)
            .ok_or(ParseResponseError::UnexpectedEndOfStream)?;
        let metadata_response_line = deserializer
            .next()
            .and_then(Result::ok)
            .ok_or(ParseResponseError::UnexpectedEndOfStream)?;

        let file_response_lines = deserializer
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ParseResponseError::BodyError {
                source: Box::new(e), // fix?
            })?;

        let lines = match (protocol_response_line, metadata_response_line) {
            (ResponseLine::Parquet(p), ResponseLine::Parquet(m)) => {
                // let protocol =
                //     p.to_protocol()
                //         .ok_or(ParseResponseError::UnexpectedWrapperObject {
                //             expected: String::from("protocol"),
                //         })?;
                // let metadata =
                //     m.to_metadata()
                //         .ok_or(ParseResponseError::UnexpectedWrapperObject {
                //             expected: String::from("metadata"),
                //         })?;

                // let files = file_response_lines
                //     .into_iter()
                //     .flat_map(|line| match line {
                //         ResponseLine::Parquet(parquet_response_line) =>
                // Some(parquet_response_line),
                //         ResponseLine::Delta(delta_response_line) => None,
                //     })
                //     .flat_map(|line| line.to_file())
                //     .collect::<Vec<_>>();

                let mut actions = vec![p, m];
                actions.extend(file_response_lines.into_iter().flat_map(|line| match line {
                    ResponseLine::Parquet(parquet_response_line) => Some(parquet_response_line),
                    ResponseLine::Delta(_) => None,
                }));

                TableChangesResponseLines::Parquet(actions)
            }
            _ => panic!("Unexpected response line"),
        };

        Ok(QueryTableChangesResponse {
            version: TableVersion(table_version),
            changes: lines,
        })
    }
}
