use bon::Builder;
use url::Url;

use super::IntoRequest;
use crate::rest::{
    request::error::RequestBuilderError,
    response::{QueryTableChangesResponse, QueryTableMetadataResponse},
};

const DELTA_SHARING_CAPABILITIES_HEADERNAME: &str = "delta-sharing-capabilities";

#[derive(Debug, Builder)]
pub struct QueryTableChangesRequest {
    url_prefix: String,
    share: String,
    schema: String,
    table: String,
    capabilities: Option<String>,
    starting_version: Option<i64>,
    ending_version: Option<i64>,
    starting_timestamp: Option<String>,
    ending_timestamp: Option<String>,
    include_historical_metadata: Option<bool>,
}

impl IntoRequest for QueryTableChangesRequest {
    type Body = ();
    type Error = RequestBuilderError;
    type Response = QueryTableChangesResponse;

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
            .push("changes");

        if self.starting_version.is_some()
            || self.ending_version.is_some()
            || self.starting_timestamp.is_some()
            || self.ending_timestamp.is_some()
            || self.include_historical_metadata.is_some()
        {
            let mut query_pairs = base_url.query_pairs_mut();
            if let Some(starting_version) = self.starting_version {
                query_pairs.append_pair("startingVersion", &starting_version.to_string());
            }
            if let Some(ending_version) = self.ending_version {
                query_pairs.append_pair("endingVersion", &ending_version.to_string());
            }
            if let Some(starting_timestamp) = self.starting_timestamp {
                query_pairs.append_pair("startingTimestamp", &starting_timestamp);
            }
            if let Some(ending_timestamp) = self.ending_timestamp {
                query_pairs.append_pair("endingTimestamp", &ending_timestamp);
            }
            if let Some(include_historical_metadata) = self.include_historical_metadata {
                query_pairs.append_pair(
                    "includeHistoricalMetadata",
                    &include_historical_metadata.to_string(),
                );
            }
        }

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
        let req = QueryTableChangesRequest::builder()
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
            "/shares/test_share/schemas/test_schema/tables/test_table/changes"
        );
        assert_eq!(req.uri().query(), None);
        assert_eq!(req.version(), Version::HTTP_11);
        assert!(req.headers().is_empty());
        assert_eq!(req.body(), &());
    }

    #[test]
    fn with_capabilities() {
        let req = QueryTableChangesRequest::builder()
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
