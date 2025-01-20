use async_trait::async_trait;
use reqwest::Response;

use crate::model::{
    action::parquet::{File, Metadata, Protocol},
    TableVersion,
};

use super::line::ResponseLine;
use super::util::{extract_delta_table_version, has_ndjson_content_type};
use super::{FromResponse, ParseResponseError};

// TODO: handle CDC when the startingVersion is set!

pub struct QueryTableDataResponse {
    pub version: TableVersion,
    pub lines: TableDataResponseLines,
}

pub enum TableDataResponseLines {
    Parquet(ParquetData),
}

pub struct ParquetData {
    pub protocol: Protocol,
    pub metadata: Metadata,
    pub files: Vec<File>,
}

#[async_trait]
impl FromResponse for QueryTableDataResponse {
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
                let protocol =
                    p.to_protocol()
                        .ok_or(ParseResponseError::UnexpectedWrapperObject {
                            expected: String::from("protocol"),
                        })?;
                let metadata =
                    m.to_metadata()
                        .ok_or(ParseResponseError::UnexpectedWrapperObject {
                            expected: String::from("metadata"),
                        })?;

                let files = file_response_lines
                    .into_iter()
                    .flat_map(|line| match line {
                        ResponseLine::Parquet(parquet_response_line) => Some(parquet_response_line),
                        ResponseLine::Delta(_) => None,
                    })
                    .flat_map(|line| line.to_file())
                    .collect::<Vec<_>>();

                let metadata_response = ParquetData {
                    protocol,
                    metadata,
                    files,
                };
                TableDataResponseLines::Parquet(metadata_response)
            }
            _ => panic!("Unexpected response line"),
        };

        Ok(QueryTableDataResponse {
            version: TableVersion(table_version),
            lines,
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
        {"metaData":{"id":"f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2","format":{"provider":"parquet"},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["date"]}}
        {"file":{"url":"https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-8b0086f2-7b27-4935-ac5a-8ed6215a6640.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010516Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=97b6762cfd8e4d7e94b9d707eff3faf266974f6e7030095c1d4a66350cfd892e","id":"8b0086f2-7b27-4935-ac5a-8ed6215a6640","partitionValues":{"date":"2021-04-28"},"size":573,"stats":"{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"nullCount\":{\"eventTime\":0}}"}}
        {"file":{"url":"https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010516Z&X-Amz-SignedHeaders=host&X-Amz-Expires=899&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=0f7acecba5df7652457164533a58004936586186c56425d9d53c52db574f6b62","id":"591723a8-6a27-4240-a90e-57426f4736d2","partitionValues":{"date":"2021-04-28"},"size":573,"stats":"{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}"}}"#;
        let response = HttpResponse::builder()
            .header("Delta-Table-Version", "3")
            .header(CONTENT_TYPE, "application/x-ndjson; charset=utf-8")
            .body(body)
            .unwrap();

        let parsed = QueryTableDataResponse::parse(response.try_into().unwrap())
            .await
            .unwrap();
        assert_eq!(parsed.version.0, 3);
    }
}
