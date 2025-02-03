use async_trait::async_trait;
use reqwest::Response;

use super::line::ResponseLine;
use super::util::{extract_delta_table_version, first_line_is_protocol, has_ndjson_content_type};
use super::{
    line::{DeltaResponseLine, ParquetResponseLine},
    util::second_line_is_metadata,
};
use super::{FromResponse, ResponseError};
use crate::model::TableVersionNumber;

pub struct QueryTableMetadataResponse {
    pub version: TableVersionNumber,
    pub lines: MetadataResponseLines,
}

pub enum MetadataResponseLines {
    Parquet(Vec<ParquetResponseLine>),
    Delta(Vec<DeltaResponseLine>),
}

#[async_trait]
impl FromResponse for QueryTableMetadataResponse {
    type Error = ResponseError;

    async fn parse(res: Response) -> Result<Self, Self::Error> {
        if !has_ndjson_content_type(res.headers()) {
            return Err(ResponseError::MissingNdJsonContentType);
        }

        let table_version = extract_delta_table_version(res.headers())?;

        let bytes = res
            .bytes()
            .await
            .map_err(|e| ResponseError::BodyError {
                source: Box::new(e),
            })?;

        let deserializer = serde_json::Deserializer::from_slice(&bytes).into_iter::<ResponseLine>();
        let lines = deserializer
            .into_iter()
            .collect::<Result<Vec<ResponseLine>, _>>()
            .map_err(|e| ResponseError::DecodeBody {
                path: String::from("UNKNOWN"),
                source: Box::new(e),
            })?;

        if !first_line_is_protocol(&lines) {
            return Err(ResponseError::UnexpectedWrapperObject {
                expected: String::from("protocol"),
            });
        }

        if !second_line_is_metadata(&lines) {
            return Err(ResponseError::UnexpectedWrapperObject {
                expected: String::from("metadata"),
            });
        }

        let meta = if lines.iter().all(|l| l.is_parquet()) {
            let parquet_lines = lines
                .into_iter()
                .flat_map(|line| line.to_parquet())
                .collect();
            MetadataResponseLines::Parquet(parquet_lines)
        } else if lines.iter().all(|line| line.is_delta()) {
            let delta_lines = lines.into_iter().flat_map(|line| line.to_delta()).collect();
            MetadataResponseLines::Delta(delta_lines)
        } else {
            return Err(ResponseError::UnexpectedWrapperObject {
                expected: String::from("protocol or metadata"),
            });
        };

        Ok(QueryTableMetadataResponse {
            version: TableVersionNumber(table_version),
            lines: meta,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use http::{header::CONTENT_TYPE, Response as HttpResponse};

    #[tokio::test]
    async fn example() {
        let body = r#"{"protocol":{"minReaderVersion":1}}
{"metaData":{"id":"f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2","format":{"provider":"parquet"},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["date"]}}"#;
        let response = HttpResponse::builder()
            .header("Delta-Table-Version", "3")
            .header(CONTENT_TYPE, "application/x-ndjson; charset=utf-8")
            .body(body)
            .unwrap();

        let parsed = QueryTableMetadataResponse::parse(response.into())
            .await
            .unwrap();
        assert_eq!(parsed.version.0, 3);
    }
}
