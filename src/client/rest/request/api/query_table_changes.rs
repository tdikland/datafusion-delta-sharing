use bon::Builder;
use bytes::Bytes;
use http::{Request, Uri};
use serde::Serialize;

use super::response::QueryTableChangesResponse;
use super::{
    make_path_and_query, IntoRequest, RequestError, DELTA_SHARING_CAPABILITIES_HEADERNAME,
};

#[derive(Debug, Builder)]
pub struct QueryTableChangesRequest<'req> {
    share: &'req str,
    schema: &'req str,
    table: &'req str,
    capabilities: Option<&'req str>,
    starting_version: Option<i64>,
    ending_version: Option<i64>,
    starting_timestamp: Option<&'req str>,
    ending_timestamp: Option<&'req str>,
    include_historical_metadata: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryTableChangesQueryParams<'req> {
    starting_version: Option<i64>,
    ending_version: Option<i64>,
    starting_timestamp: Option<&'req str>,
    ending_timestamp: Option<&'req str>,
    include_historical_metadata: Option<bool>,
}

impl IntoRequest for QueryTableChangesRequest<'_> {
    type Response = QueryTableChangesResponse;

    fn into_request(self) -> Result<Request<Bytes>, RequestError> {
        let path = format!(
            "/shares/{}/schemas/{}/tables/{}/changes",
            self.share, self.schema, self.table
        );
        let query = QueryTableChangesQueryParams {
            starting_version: self.starting_version,
            ending_version: self.ending_version,
            starting_timestamp: self.starting_timestamp,
            ending_timestamp: self.ending_timestamp,
            include_historical_metadata: self.include_historical_metadata,
        };

        let path_and_query = make_path_and_query(path, query)?;
        let uri = Uri::builder().path_and_query(path_and_query).build()?;
        let mut req = Request::builder().uri(uri);
        if let Some(cap) = self.capabilities {
            req = req.header(DELTA_SHARING_CAPABILITIES_HEADERNAME, cap);
        }
        let req = req.body(Bytes::new())?;

        Ok(req)
    }
}

#[cfg(test)]
mod test {
    use http::{Method, Version};

    use super::*;

    #[test]
    fn example() {
        let req = QueryTableChangesRequest::builder()
            .share("test_share")
            .schema("test_schema")
            .table("test_table")
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
        assert!(req.body().is_empty());
    }

    #[test]
    fn with_capabilities() {
        let req = QueryTableChangesRequest::builder()
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
