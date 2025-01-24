use bon::Builder;
use http::{header::CONTENT_TYPE, Request};
use serde::Serialize;

use crate::rest::response::ListSharesResponse;

use super::IntoRequest;

#[derive(Debug, Builder)]
pub struct ListSharesRequest<'req> {
    url_prefix: &'req str,
    max_results: Option<i32>,
    page_token: Option<&'req str>,
}

#[derive(Debug, Serialize)]
pub struct ListSharesQueryParams<'req> {
    max_results: Option<i32>,
    page_token: Option<&'req str>,
}

impl IntoRequest for ListSharesRequest {
    type Response = ListSharesResponse;

    fn into_request(self) -> http::Request<Self::Body> {
        let endpoint = format!("{}/shares", self.url_prefix);
        let query = ListSharesQueryParams {
            max_results: self.max_results,
            page_token: self.page_token,
        };

        Request::get(endpoint).body(()).expect("valid request")
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Write;

    use http::Request;
    use insta::{assert_json_snapshot, assert_snapshot};

    use super::*;

    #[test]
    fn example() {
        let request = ListSharesRequest::builder()
            .url_prefix(String::from("https://server.com/api"))
            .max_results(1)
            .page_token(String::from("page1"))
            .build()
            .into_request();
        assert_snapshot!(render(&request));
    }

    fn render<T>(request: &Request<T>) -> String
    where
        T: Serialize,
    {
        let mut result = String::new();

        // Start with the request line
        writeln!(
            &mut result,
            "{} {} HTTP/1.1",
            request.method(),
            request.uri()
        )
        .unwrap();

        // Add headers
        for (key, value) in request.headers() {
            writeln!(
                &mut result,
                "{}: {}",
                key,
                value.to_str().unwrap_or("<invalid UTF-8>")
            )
            .unwrap();
        }

        // Separate headers from the body with an empty line
        writeln!(&mut result).unwrap();

        // Add body if present
        let body = serde_json::to_string(request.body()).unwrap();
        if !body.is_empty() {
            writeln!(&mut result, "{}", body).unwrap();
        }

        result
    }

    #[derive(Debug, Serialize)]
    #[serde(transparent)]
    struct WrappedRequest<T: Serialize> {
        #[serde(with = "http_serde_ext::request")]
        inner: Request<T>,
    }

    trait IntoWrappedRequest<T: Serialize>: IntoRequest<Body = T> {
        fn into_wrapped_request(self) -> WrappedRequest<T>;
    }

    impl<U: Serialize, T: IntoRequest<Body = U>> IntoWrappedRequest<U> for T {
        fn into_wrapped_request(self) -> WrappedRequest<U> {
            WrappedRequest {
                inner: self.into_request(),
            }
        }
    }
}
