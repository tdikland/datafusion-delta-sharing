use std::borrow::Cow;

use bon::Builder;
use http::{HeaderMap, Method};
use serde::{Deserialize, Serialize};

use crate::model::Share;

use super::response::{GetShareResponse, ListSchemasResponse, ListSharesResponse, ListTablesResponse};

pub enum RequestBody<T> {
    Empty,
    Json(T),
}

pub trait Request
where
    Self: Sized,
{
    type Body: Serialize;
    type Query: Serialize;
    type Response: for<'de> Deserialize<'de>;

    const HTTP_METHOD: Method;

    fn endpoint(&self) -> Cow<'_, str>;

    fn headers(&self) -> HeaderMap {
        HeaderMap::new()
    }

    fn query(&self) -> Option<Self::Query> {
        None
    }

    fn body(self) -> RequestBody<Self::Body> {
        RequestBody::Empty
    }
}

#[derive(Debug, Serialize)]
pub struct Pagination {
    max_results: Option<String>,
    page_token: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
//// LIST SHARES REQUEST                                                    ////
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Builder)]
pub struct ListSharesRequest {
    max_results: Option<u32>,
    #[builder(into)]
    page_token: Option<String>,
}

impl Request for ListSharesRequest {
    type Body = ();
    type Query = Pagination;
    type Response = ListSharesResponse;

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        "/shares".into()
    }
}

////////////////////////////////////////////////////////////////////////////////
//// GET SHARE REQUEST                                                      ////
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Builder)]
pub struct GetShareRequest {
    #[builder(into)]
    share_name: String,
}

impl Request for GetShareRequest {
    type Body = ();
    type Query = ();
    type Response = GetShareResponse;

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!("/shares/{}", self.share_name).into()
    }
}

////////////////////////////////////////////////////////////////////////////////
//// LIST SCHEMAS                                                           ////
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Builder)]
pub struct ListSchemasRequest {
    share_name: String,
    max_results: Option<u32>,
    page_token: Option<String>
}

impl Request for ListSchemasRequest {
    type Body = ();
    type Query = Pagination;
    type Response = ListSchemasResponse;

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!("/shares/{}/schemas", self.share_name).into()
    }
}

////////////////////////////////////////////////////////////////////////////////
//// LIST TABLES IN SCHEMA                                                  ////
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Builder)]
pub struct ListTablesInSchemaRequest {
    share_name: String,
    schema_name: String,
    max_results: Option<u32>,
    page_token: Option<String>
}

impl Request for ListTablesInSchemaRequest {
    type Body = ();
    type Query = Pagination;
    type Response = ListTablesResponse;

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!(
            "/shares/{}/schemas/{}/tables",
            self.share_name, self.schema_name
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////
//// LIST TABLES IN SHARE                                                   ////
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Builder)]
pub struct ListTablesInShareRequest {
    share_name: String,
    max_results: Option<u32>,
    page_token: Option<String>
}

impl Request for ListTablesInShareRequest {
    type Body = ();
    type Query = Pagination;
    type Response = ListTablesResponse;

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!("/shares/{}/all-tables", self.share_name).into()
    }
}

////////////////////////////////////////////////////////////////////////////////
//// QUERY TABLE VERSION                                                    ////
////////////////////////////////////////////////////////////////////////////////

pub struct QueryTableVersionRequest {
    share_name: String,
    table_name: String,
    version: String,
}

impl Request for QueryTableVersionRequest {
    type Body = ();
    type Query = ();
    type Response = ();

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!(
            "/shares/{}/tables/{}/versions/{}",
            self.share_name, self.table_name, self.version
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////
//// QUERY TABLE METADATA                                                   ////
////////////////////////////////////////////////////////////////////////////////

pub struct QueryTableMetadataRequest {
    share_name: String,
    table_name: String,
}

impl Request for QueryTableMetadataRequest {
    type Body = ();
    type Query = ();
    type Response = ();

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!("/shares/{}/tables/{}", self.share_name, self.table_name).into()
    }
}

////////////////////////////////////////////////////////////////////////////////
//// QUERY TABLE DATA                                                       ////
////////////////////////////////////////////////////////////////////////////////

pub struct QueryTableDataRequest {
    share_name: String,
    table_name: String,
    version: Option<String>,
    pagination: Pagination,
}

impl Request for QueryTableDataRequest {
    type Body = ();
    type Query = Pagination;
    type Response = ();

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!(
            "/shares/{}/tables/{}/data",
            self.share_name, self.table_name
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////
//// QUERY TABLE CHANGES                                                    ////
////////////////////////////////////////////////////////////////////////////////

pub struct QueryTableChangesRequest {
    share_name: String,
    table_name: String,
    version: Option<String>,
    pagination: Pagination,
}

impl Request for QueryTableChangesRequest {
    type Body = ();
    type Query = Pagination;
    type Response = ();

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!(
            "/shares/{}/tables/{}/changes",
            self.share_name, self.table_name
        )
        .into()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use insta::assert_json_snapshot;
    use reqwest::Client;

    use super::*;

    #[derive(Debug, Serialize)]
    struct RequestSnap<Q, B> {
        method: String,
        endpoint: String,
        headers: HashMap<String, String>,
        query: Option<Q>,
        body: Option<B>,
    }

    fn into_delta_sharing_client_request<Q, B, R: Request<Query = Q, Body = B>>(
        request: R,
    ) -> RequestSnap<Q, B> {
        RequestSnap {
            method: R::HTTP_METHOD.to_string(),
            endpoint: request.endpoint().into_owned(),
            headers: request
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
                .collect(),
            query: request.query(),
            body: match request.body() {
                RequestBody::Empty => None,
                RequestBody::Json(body) => Some(body),
            },
        }
    }

    #[test]
    fn test_list_shares_request() {
        let request = ListSharesRequest::builder()
            .max_results(10)
            .page_token("foo")
            .build();
        assert_json_snapshot!(into_delta_sharing_client_request(request));
    }
}
