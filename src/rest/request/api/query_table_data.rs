use bon::Builder;
use http::{header::CONTENT_TYPE, Method};
use serde::Serialize;
use url::Url;

use super::{IntoRequest, RequestBuilderError};
use crate::rest::response::QueryTableDataResponse;

const DELTA_SHARING_CAPABILITIES_HEADERNAME: &str = "delta-sharing-capabilities";

#[derive(Debug, Builder)]
pub struct QueryTableDataRequest {
    url_prefix: String,
    share: String,
    schema: String,
    table: String,
    capabilities: Option<String>,
    predicate_hints: Option<String>,
    json_predicate_hints: Option<String>,
    limit_hint: Option<i32>,
    version: Option<i64>,
    timestamp: Option<String>,
    starting_version: Option<i64>,
    ending_version: Option<i64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryTableDataBody {
    predicate_hints: Option<String>,
    json_predicate_hints: Option<String>,
    limit_hint: Option<i32>,
    version: Option<i64>,
    timestamp: Option<String>,
    starting_version: Option<i64>,
    ending_version: Option<i64>,
}

impl IntoRequest for QueryTableDataRequest {
    type Body = QueryTableDataBody;
    type Error = RequestBuilderError;
    type Response = QueryTableDataResponse;

    fn into_request(self) -> Result<http::Request<Self::Body>, Self::Error> {
        let mut base_url = self.url_prefix.parse::<Url>().unwrap();
        base_url
            .path_segments_mut()
            .unwrap()
            .push("shares")
            .push(&self.share)
            .push("schemas")
            .push(&self.schema)
            .push("tables")
            .push(&self.table)
            .push("query");

        let mut req = http::Request::builder()
            .uri(base_url.to_string())
            .method(Method::POST);
        if let Some(cap) = self.capabilities {
            req = req.header(DELTA_SHARING_CAPABILITIES_HEADERNAME, cap);
        }

        let body = QueryTableDataBody {
            predicate_hints: self.predicate_hints,
            json_predicate_hints: self.json_predicate_hints,
            limit_hint: self.limit_hint,
            version: self.version,
            timestamp: self.timestamp,
            starting_version: self.starting_version,
            ending_version: self.ending_version,
        };
        req = req.header(CONTENT_TYPE, "application/json; charset=utf-8");

        req.body(body).map_err(RequestBuilderError::from)
    }
}

#[cfg(test)]
mod test {
    use http::{Method, Version};

    use super::*;

    #[test]
    fn example() {
        let req = QueryTableDataRequest::builder()
            .url_prefix(String::from("https://server.com"))
            .share(String::from("test_share"))
            .schema(String::from("test_schema"))
            .table(String::from("test_table"))
            .json_predicate_hints(String::from("json_predicate_hints"))
            .limit_hint(100)
            .version(5)
            .timestamp(String::from("2021-01-01T00:00:00Z"))
            .starting_version(1)
            .ending_version(10)
            .build()
            .into_request()
            .unwrap();

        assert_eq!(req.method(), Method::POST);
        assert_eq!(
            req.uri().path(),
            "/shares/test_share/schemas/test_schema/tables/test_table/query"
        );
        assert_eq!(req.uri().query(), None);
        assert_eq!(req.version(), Version::HTTP_11);
        assert_eq!(req.headers().len(), 1);
        assert_eq!(
            req.headers()
                .get("content-type")
                .map(|v| v.to_str().unwrap()),
            Some("application/json; charset=utf-8")
        );
        assert_eq!(
            req.body().json_predicate_hints,
            Some(String::from("json_predicate_hints"))
        );
        assert_eq!(req.body().limit_hint, Some(100));
        assert_eq!(req.body().version, Some(5));
        assert_eq!(
            req.body().timestamp,
            Some(String::from("2021-01-01T00:00:00Z"))
        );
        assert_eq!(req.body().starting_version, Some(1));
        assert_eq!(req.body().ending_version, Some(10));
    }

    #[test]
    fn with_capabilities() {
        let req = QueryTableDataRequest::builder()
            .url_prefix(String::from("https://server.com"))
            .share(String::from("test_share"))
            .schema(String::from("test_schema"))
            .table(String::from("test_table"))
            .capabilities(String::from(
                "responseformat=delta;readerfeatures=deletionvectors",
            ))
            .build()
            .into_request()
            .unwrap();

        assert_eq!(req.headers().len(), 2);
        assert_eq!(
            req.headers()
                .get("delta-sharing-capabilities")
                .map(|v| v.to_str().unwrap()),
            Some("responseformat=delta;readerfeatures=deletionvectors")
        );
    }
}
