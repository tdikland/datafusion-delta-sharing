use bon::Builder;
use bytes::Bytes;
use http::{Request, Uri};
use serde::Serialize;

use super::response::ListTablesResponse;
use super::{make_path_and_query, IntoRequest, RequestError};

#[derive(Debug, Builder)]
pub struct ListTablesInSchemaRequest<'req> {
    share: &'req str,
    schema: &'req str,
    max_results: Option<i32>,
    page_token: Option<&'req str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ListTablesInSchemaQueryParams<'req> {
    max_results: Option<i32>,
    page_token: Option<&'req str>,
}

impl IntoRequest for ListTablesInSchemaRequest<'_> {
    type Response = ListTablesResponse;

    fn into_request(self) -> Result<Request<Bytes>, RequestError> {
        let path = format!("/shares/{}/schemas/{}/tables", self.share, self.schema);
        let query = ListTablesInSchemaQueryParams {
            max_results: self.max_results,
            page_token: self.page_token,
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
        let req = ListTablesInSchemaRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .max_results(1)
            .page_token("foo")
            .build()
            .into_request()
            .unwrap();

        assert_eq!(req.method(), Method::GET);
        assert_eq!(
            req.uri().path(),
            "/shares/test_share/schemas/test_schema/tables"
        );
        assert_eq!(req.uri().query(), Some("maxResults=1&pageToken=foo"));
        assert_eq!(req.version(), Version::HTTP_11);
        assert!(req.headers().is_empty());
        assert!(req.body().is_empty());
    }

    #[test]
    fn pagination_params() {
        let req_no_params = ListTablesInSchemaRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .build()
            .into_request()
            .unwrap();
        assert_eq!(req_no_params.uri().query(), None);

        let req_only_max_results = ListTablesInSchemaRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .max_results(100)
            .build()
            .into_request()
            .unwrap();
        assert_eq!(req_only_max_results.uri().query(), Some("maxResults=100"));

        let req_only_page_token = ListTablesInSchemaRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .page_token("foo")
            .build()
            .into_request()
            .unwrap();
        assert_eq!(req_only_page_token.uri().query(), Some("pageToken=foo"));

        let req_both_params = ListTablesInSchemaRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .page_token("foo")
            .max_results(100)
            .build()
            .into_request()
            .unwrap();
        assert_eq!(
            req_both_params.uri().query(),
            Some("maxResults=100&pageToken=foo")
        );
    }
}
