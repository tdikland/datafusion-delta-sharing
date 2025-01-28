use bon::Builder;
use url::Url;

use super::response::GetShareResponse;
use super::{IntoRequest, RequestBuilderError};

#[derive(Debug, Builder)]
pub struct GetShareRequest {
    url_prefix: String,
    share_name: String,
}

impl IntoRequest for GetShareRequest {
    type Body = ();
    type Error = RequestBuilderError;
    type Response = GetShareResponse;

    fn into_request(self) -> Result<http::Request<Self::Body>, Self::Error> {
        let mut base_url = self.url_prefix.parse::<Url>().unwrap();
        base_url.path_segments_mut().unwrap().push("shares");
        base_url.path_segments_mut().unwrap().push(&self.share_name);
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
        let req = GetShareRequest::builder()
            .url_prefix(String::from("https://server.com"))
            .share_name(String::from("test_share"))
            .build()
            .into_request()
            .unwrap();

        assert_eq!(req.method(), Method::GET);
        assert_eq!(req.uri().path(), "/shares/test_share");
        assert!(req.uri().query().is_none());
        assert_eq!(req.version(), Version::HTTP_11);
        assert!(req.headers().is_empty());
        assert_eq!(req.body(), &());
    }
}
