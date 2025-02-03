use bon::Builder;
use bytes::Bytes;
use http::{Request, Uri};
use serde::Serialize;

use super::response::QueryTableVersionResponse;
use super::{make_path_and_query, IntoRequest, RequestError};

#[derive(Debug, Builder)]
pub struct QueryTableVersionRequest<'req> {
    share: &'req str,
    schema: &'req str,
    table: &'req str,
    starting_timestamp: Option<&'req str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryTableVersionQueryParams<'req> {
    starting_timestamp: Option<&'req str>,
}

impl IntoRequest for QueryTableVersionRequest<'_> {
    type Response = QueryTableVersionResponse;

    fn into_request(self) -> Result<Request<Bytes>, RequestError> {
        let path = format!(
            "/shares/{}/schemas/{}/tables/{}/version",
            self.share, self.schema, self.table
        );
        let query = QueryTableVersionQueryParams {
            starting_timestamp: self.starting_timestamp,
        };

        let path_and_query = make_path_and_query(path, query)?;
        let uri = Uri::builder().path_and_query(path_and_query).build()?;
        let req = Request::builder().uri(uri).body(Bytes::new())?;
        Ok(req)
    }
}

#[cfg(test)]
mod test {
    use http::{Method, Version};

    use super::*;

    #[test]
    fn example() {
        let req = QueryTableVersionRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .table("test_table")
            .starting_timestamp("2022-01-01T00:00:00Z")
            .build()
            .into_request()
            .unwrap();

        assert_eq!(req.method(), Method::GET);
        assert_eq!(
            req.uri().path(),
            "/shares/test_share/schemas/test_schema/tables/test_table/version"
        );
        assert_eq!(
            req.uri().query(),
            Some("startingTimestamp=2022-01-01T00%3A00%3A00Z")
        );
        assert_eq!(req.version(), Version::HTTP_11);
        assert!(req.headers().is_empty());
        assert!(req.body().is_empty());
    }

    #[test]
    fn without_starting_timestamp() {
        let req = QueryTableVersionRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .table("test_table")
            .build()
            .into_request()
            .unwrap();

        assert_eq!(req.uri().query(), None);
    }
}
