use bon::Builder;
use url::Url;

use super::IntoRequest;
use crate::rest::{request::error::RequestBuilderError, response::QueryTableVersionResponse};

#[derive(Debug, Builder)]
pub struct QueryTableVersionRequest {
    url_prefix: String,
    share: String,
    schema: String,
    table: String,
    starting_timestamp: Option<String>,
}

impl IntoRequest for QueryTableVersionRequest {
    type Body = ();
    type Error = RequestBuilderError;
    type Response = QueryTableVersionResponse;

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
            .push("version");

        if self.starting_timestamp.is_some() {
            let mut query_pairs = base_url.query_pairs_mut();
            if let Some(ts) = self.starting_timestamp {
                query_pairs.append_pair("startingTimestamp", &ts);
            }
        }

        let req = http::Request::builder().uri(base_url.to_string());
        Ok(req.body(()).expect("valid"))
    }
}

#[cfg(test)]
mod test {
    use http::{Method, Version};

    use super::*;

    #[test]
    fn example() {
        let req = QueryTableVersionRequest::builder()
            .url_prefix(String::from("https://server.com"))
            .share(String::from("test_share"))
            .schema(String::from("test_schema"))
            .table(String::from("test_table"))
            .starting_timestamp(String::from("2022-01-01T00:00:00Z"))
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
        assert_eq!(req.body(), &());
    }

    #[test]
    fn without_starting_timestamp() {
        let req = QueryTableVersionRequest::builder()
            .url_prefix(String::from("https://server.com"))
            .share(String::from("test_share"))
            .schema(String::from("test_schema"))
            .table(String::from("test_table"))
            .build()
            .into_request()
            .unwrap();

        assert_eq!(req.uri().query(), None);
    }
}
