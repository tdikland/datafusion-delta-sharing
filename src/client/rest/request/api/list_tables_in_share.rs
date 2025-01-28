use bon::Builder;
use url::Url;

use super::response::ListTablesResponse;
use super::{IntoRequest, RequestBuilderError};

#[derive(Debug, Builder)]
pub struct ListTablesInShareRequest {
    url_prefix: String,
    share: String,
    max_results: Option<i32>,
    page_token: Option<String>,
}

impl IntoRequest for ListTablesInShareRequest {
    type Body = ();
    type Error = RequestBuilderError;
    type Response = ListTablesResponse;

    fn into_request(self) -> Result<http::Request<Self::Body>, Self::Error> {
        let mut base_url = self.url_prefix.parse::<Url>().unwrap();
        base_url
            .path_segments_mut()
            .unwrap()
            .push("shares")
            .push(&self.share)
            .push("all-tables");
        if self.max_results.is_some() || self.page_token.is_some() {
            let mut query_pairs = base_url.query_pairs_mut();
            if let Some(max) = self.max_results {
                query_pairs.append_pair("maxResults", &max.to_string());
            }
            if let Some(token) = self.page_token {
                query_pairs.append_pair("pageToken", &token);
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
        let req = ListTablesInShareRequest::builder()
            .url_prefix(String::from("https://server.com"))
            .share(String::from("test_share"))
            .max_results(1)
            .page_token(String::from("token"))
            .build()
            .into_request()
            .unwrap();

        assert_eq!(req.method(), Method::GET);
        assert_eq!(req.uri().path(), "/shares/test_share/all-tables");
        assert_eq!(req.uri().query(), Some("maxResults=1&pageToken=token"));
        assert_eq!(req.version(), Version::HTTP_11);
        assert!(req.headers().is_empty());
        assert_eq!(req.body(), &());
    }

    #[test]
    fn pagination_params() {
        let req_no_params = ListTablesInShareRequest::builder()
            .url_prefix(String::from("https://server.com"))
            .share(String::from("test_share"))
            .build()
            .into_request()
            .unwrap();
        assert_eq!(req_no_params.uri().query(), None);

        let req_only_max_results = ListTablesInShareRequest::builder()
            .url_prefix(String::from("https://server.com"))
            .share(String::from("test_share"))
            .max_results(100)
            .build()
            .into_request()
            .unwrap();
        assert_eq!(req_only_max_results.uri().query(), Some("maxResults=100"));

        let req_only_page_token = ListTablesInShareRequest::builder()
            .url_prefix(String::from("https://server.com"))
            .share(String::from("test_share"))
            .page_token(String::from("foo"))
            .build()
            .into_request()
            .unwrap();
        assert_eq!(req_only_page_token.uri().query(), Some("pageToken=foo"));

        let req_both_params = ListTablesInShareRequest::builder()
            .url_prefix(String::from("https://server.com"))
            .share(String::from("test_share"))
            .page_token(String::from("foo"))
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
