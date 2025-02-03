use super::response::QueryTableDataResponse;
use super::{IntoRequest, RequestError, DELTA_SHARING_CAPABILITIES_HEADERNAME};
use bon::Builder;
use bytes::Bytes;
use http::Uri;
use http::{header::CONTENT_TYPE, Method};
use serde::{Deserialize, Serialize};

#[derive(Debug, Builder)]
pub struct QueryTableDataRequest<'req> {
    share: &'req str,
    schema: &'req str,
    table: &'req str,
    capabilities: Option<String>,
    predicate_hints: Option<String>,
    json_predicate_hints: Option<String>,
    limit_hint: Option<i32>,
    version: Option<i64>,
    timestamp: Option<String>,
    starting_version: Option<i64>,
    ending_version: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryTableDataBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    predicate_hints: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    json_predicate_hints: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit_hint: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    starting_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ending_version: Option<i64>,
}

impl IntoRequest for QueryTableDataRequest<'_> {
    type Response = QueryTableDataResponse;

    fn into_request(self) -> Result<http::Request<Bytes>, RequestError> {
        let path = format!(
            "/shares/{}/schemas/{}/tables/{}/query",
            self.share, self.schema, self.table
        );

        let uri = Uri::builder().path_and_query(path).build()?;
        let mut req = http::Request::builder().uri(uri).method(Method::POST);
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
        let body =
            serde_json::to_vec(&body).map_err(|e| RequestError::SerializeBody(e.to_string()))?;
        let req = req.body(body.into())?;

        Ok(req)
    }
}

#[cfg(test)]
mod test {
    use http::{Method, Version};

    use super::*;

    #[test]
    #[ignore = "todo"]
    fn example() {
        let req = QueryTableDataRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .table("test_table")
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

        panic!();
        // TODO
        // assert_eq!(
        //     req.body().json_predicate_hints,
        //     Some(String::from("json_predicate_hints"))
        // );
        // assert_eq!(req.body().limit_hint, Some(100));
        // assert_eq!(req.body().version, Some(5));
        // assert_eq!(
        //     req.body().timestamp,
        //     Some(String::from("2021-01-01T00:00:00Z"))
        // );
        // assert_eq!(req.body().starting_version, Some(1));
        // assert_eq!(req.body().ending_version, Some(10));
    }

    #[test]
    fn with_capabilities() {
        let req = QueryTableDataRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .table("test_table")
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

    #[test]
    fn empty_body() {
        let req = QueryTableDataRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .table("test_table")
            .build()
            .into_request()
            .unwrap();

        assert_eq!(req.body(), "{}".as_bytes())
    }
}
