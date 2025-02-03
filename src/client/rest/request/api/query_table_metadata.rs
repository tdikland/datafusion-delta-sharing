use bon::Builder;
use bytes::Bytes;
use http::{Request, Uri};

use super::response::QueryTableMetadataResponse;
use super::{IntoRequest, RequestError};

const DELTA_SHARING_CAPABILITIES_HEADERNAME: &str = "delta-sharing-capabilities";

#[derive(Debug, Builder)]
pub struct QueryTableMetadataRequest<'req> {
    share: &'req str,
    schema: &'req str,
    table: &'req str,
    capabilities: Option<&'req str>,
}

impl IntoRequest for QueryTableMetadataRequest<'_> {
    type Response = QueryTableMetadataResponse;

    fn into_request(self) -> Result<http::Request<Bytes>, RequestError> {
        let path = format!(
            "/shares/{}/schemas/{}/tables/{}/metadata",
            self.share, self.schema, self.table
        );

        let uri = Uri::builder().path_and_query(path).build()?;
        let mut req = Request::builder().uri(uri);
        if let Some(cap) = self.capabilities {
            req = req.header(DELTA_SHARING_CAPABILITIES_HEADERNAME, cap);
        }
        Ok(req.body(Bytes::new())?)
    }
}

#[cfg(test)]
mod test {
    use http::{Method, Version};

    use super::*;

    #[test]
    fn example() {
        let req = QueryTableMetadataRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .table("test_table")
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
        assert!(req.body().is_empty());
    }

    #[test]
    fn with_capabilities() {
        let req = QueryTableMetadataRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .table("test_table")
            .capabilities("responseformat=delta;readerfeatures=deletionvectors")
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
