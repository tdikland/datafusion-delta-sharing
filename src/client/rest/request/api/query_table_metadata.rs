use bon::Builder;
use url::Url;

use super::response::QueryTableMetadataResponse;
use super::{IntoRequest, RequestBuilderError};

const DELTA_SHARING_CAPABILITIES_HEADERNAME: &str = "delta-sharing-capabilities";

#[derive(Debug, Builder)]
pub struct QueryTableMetadataRequest {
    url_prefix: String,
    share: String,
    schema: String,
    table: String,
    capabilities: Option<String>,
}

impl IntoRequest for QueryTableMetadataRequest {
    type Body = ();
    type Error = RequestBuilderError;
    type Response = QueryTableMetadataResponse;

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
            .push("metadata");

        let mut req = http::Request::builder().uri(base_url.to_string());
        if let Some(cap) = self.capabilities {
            req = req.header(DELTA_SHARING_CAPABILITIES_HEADERNAME, cap);
        }

        Ok(req.body(()).expect("valid"))
    }
}

#[cfg(test)]
mod test {
    use http::{Method, Version};

    use super::*;

    #[test]
    fn example() {
        let req = QueryTableMetadataRequest::builder()
            .url_prefix(String::from("https://server.com"))
            .share(String::from("test_share"))
            .schema(String::from("test_schema"))
            .table(String::from("test_table"))
            .build()
            .into_request()
            .unwrap();

        assert_eq!(req.method(), Method::GET);
        assert_eq!(
            req.uri().path(),
            "/shares/test_share/schemas/test_schema/tables/test_table/metadata"
        );
        assert_eq!(req.uri().query(), None);
        assert_eq!(req.version(), Version::HTTP_11);
        assert!(req.headers().is_empty());
        assert_eq!(req.body(), &());
    }

    #[test]
    fn with_capabilities() {
        let req = QueryTableMetadataRequest::builder()
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

        assert_eq!(req.headers().len(), 1);
        assert_eq!(
            req.headers()
                .get("delta-sharing-capabilities")
                .map(|v| v.to_str().unwrap()),
            Some("responseformat=delta;readerfeatures=deletionvectors")
        );
    }
}
