use std::borrow::Cow;

use bon::Builder;
use http::{HeaderMap, Method};
use reqwest::Body;
// use reqwest::Request;
use serde::Serialize;

use super::response::{
    FromResponse, GetShareResponse, ListSchemasResponse, ListSharesResponse, ListTablesResponse,
    QueryTableChangesResponse, QueryTableDataResponse, QueryTableMetadataResponse,
    QueryTableVersionResponse,
};

// mod api;

trait IntoRequest<T>: Sized {
    type Error;
    type Response: FromResponse;

    fn into_request(self) -> http::Request<T>;
}

// fn exec<R: IntoRequest>(client: reqwest::Client, req: R) {
//     let r: reqwest::Request = req
//         .into_request()
//         .map(|body| serde_json::to_string(&body).unwrap())
//         .try_into()
//         .unwrap();
// }

pub(crate) trait Request
where
    Self: Sized,
{
    type Body: Serialize;
    type Query: Serialize;
    type Response: FromResponse;

    const HTTP_METHOD: Method;

    fn endpoint(&self) -> Cow<'_, str>;

    fn headers(&self) -> HeaderMap {
        HeaderMap::new()
    }

    fn query(&self) -> Option<Self::Query> {
        None
    }

    fn body(self) -> Option<Self::Body> {
        None
    }
}

////////////////////////////////////////////////////////////////////////////////
// LIST SHARES REQUEST
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Builder)]
pub struct ListSharesRequest {
    max_results: Option<i32>,
    page_token: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListSharesQueryParams {
    max_results: Option<i32>,
    page_token: Option<String>,
}

impl Request for ListSharesRequest {
    type Body = ();
    type Query = ListSharesQueryParams;
    type Response = ListSharesResponse;

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        "/shares".into()
    }

    fn query(&self) -> Option<Self::Query> {
        Some(ListSharesQueryParams {
            max_results: self.max_results.clone(),
            page_token: self.page_token.clone(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
//// GET SHARE REQUEST                                                      ////
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Builder)]
pub struct GetShareRequest {
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
    max_results: Option<i32>,
    page_token: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListSchemasQueryParams {
    max_results: Option<i32>,
    page_token: Option<String>,
}

impl Request for ListSchemasRequest {
    type Body = ();
    type Query = ListSchemasQueryParams;
    type Response = ListSchemasResponse;

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!("/shares/{}/schemas", self.share_name).into()
    }

    fn query(&self) -> Option<Self::Query> {
        Some(ListSchemasQueryParams {
            max_results: self.max_results.clone(),
            page_token: self.page_token.clone(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
//// LIST TABLES IN SCHEMA                                                  ////
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Builder)]
pub struct ListTablesInSchemaRequest {
    share_name: String,
    schema_name: String,
    max_results: Option<i32>,
    page_token: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListTablesInSchemaQueryParams {
    max_results: Option<i32>,
    page_token: Option<String>,
}

impl Request for ListTablesInSchemaRequest {
    type Body = ();
    type Query = ListTablesInSchemaQueryParams;
    type Response = ListTablesResponse;

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!(
            "/shares/{}/schemas/{}/tables",
            self.share_name, self.schema_name
        )
        .into()
    }

    fn query(&self) -> Option<Self::Query> {
        Some(ListTablesInSchemaQueryParams {
            max_results: self.max_results.clone(),
            page_token: self.page_token.clone(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
//// LIST TABLES IN SHARE                                                   ////
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Builder)]
pub struct ListTablesInShareRequest {
    share_name: String,
    max_results: Option<i32>,
    page_token: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListTablesInShareQueryParams {
    max_results: Option<i32>,
    page_token: Option<String>,
}

impl Request for ListTablesInShareRequest {
    type Body = ();
    type Query = ListTablesInShareQueryParams;
    type Response = ListTablesResponse;

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!("/shares/{}/all-tables", self.share_name).into()
    }

    fn query(&self) -> Option<Self::Query> {
        Some(ListTablesInShareQueryParams {
            max_results: self.max_results.clone(),
            page_token: self.page_token.clone(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
//// QUERY TABLE VERSION                                                    ////
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Builder)]
pub struct QueryTableVersionRequest {
    share_name: String,
    schema_name: String,
    table_name: String,
    starting_timestamp: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct QueryTableVersionQueryParams {
    starting_timestamp: Option<String>,
}

impl Request for QueryTableVersionRequest {
    type Body = ();
    type Query = QueryTableVersionQueryParams;
    type Response = QueryTableVersionResponse;

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!(
            "/shares/{}/schemas/{}/tables/{}/version",
            self.share_name, self.schema_name, self.table_name
        )
        .into()
    }

    fn query(&self) -> Option<Self::Query> {
        Some(QueryTableVersionQueryParams {
            starting_timestamp: self.starting_timestamp.clone(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
//// QUERY TABLE METADATA                                                   ////
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Builder)]
pub struct QueryTableMetadataRequest {
    share_name: String,
    schema_name: String,
    table_name: String,
}

impl Request for QueryTableMetadataRequest {
    type Body = ();
    type Query = ();
    type Response = QueryTableMetadataResponse;

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!(
            "/shares/{}/schemas/{}/tables/{}",
            self.share_name, self.schema_name, self.table_name
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////
// QUERY TABLE DATA
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Builder)]
pub struct QueryTableDataRequest {
    share_name: String,
    schema_name: String,
    table_name: String,
    json_predicate_hints: Option<String>,
    limit_hint: Option<i32>,
    version: Option<i64>,
    timestamp: Option<String>,
    starting_version: Option<i64>,
    ending_version: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct QueryTableDataBody {
    json_predicate_hints: Option<String>,
    limit_hint: Option<i32>,
    version: Option<i64>,
    timestamp: Option<String>,
    starting_version: Option<i64>,
    ending_version: Option<i64>,
}

impl Request for QueryTableDataRequest {
    type Body = QueryTableDataBody;
    type Query = ();
    type Response = QueryTableDataResponse;

    const HTTP_METHOD: Method = Method::POST;

    fn endpoint(&self) -> Cow<'_, str> {
        format!(
            "/shares/{}/schema/{}/tables/{}/query",
            self.share_name, self.schema_name, self.table_name
        )
        .into()
    }

    fn body(self) -> Option<Self::Body> {
        Some(QueryTableDataBody {
            json_predicate_hints: self.json_predicate_hints,
            limit_hint: self.limit_hint,
            version: self.version,
            timestamp: self.timestamp,
            starting_version: self.starting_version,
            ending_version: self.ending_version,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
// QUERY TABLE CHANGES
////////////////////////////////////////////////////////////////////////////////

pub struct QueryTableChangesRequest {
    share_name: String,
    schema_name: String,
    table_name: String,
    starting_version: Option<i32>,
    starting_timestamp: Option<String>,
    ending_version: Option<i32>,
    ending_timestamp: Option<String>,
    include_historical_metadata: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct QueryTableChangesQueryParams {
    starting_version: Option<i32>,
    starting_timestamp: Option<String>,
    ending_version: Option<i32>,
    ending_timestamp: Option<String>,
    include_historical_metadata: Option<bool>,
}

impl Request for QueryTableChangesRequest {
    type Body = ();
    type Query = QueryTableChangesQueryParams;
    type Response = QueryTableChangesResponse;

    const HTTP_METHOD: Method = Method::GET;

    fn endpoint(&self) -> Cow<'_, str> {
        format!(
            "/shares/{}/schemas/{}/tables/{}/changes",
            self.share_name, self.schema_name, self.table_name
        )
        .into()
    }

    fn query(&self) -> Option<Self::Query> {
        Some(QueryTableChangesQueryParams {
            starting_version: self.starting_version,
            starting_timestamp: self.starting_timestamp.clone(),
            ending_version: self.ending_version,
            ending_timestamp: self.ending_timestamp.clone(),
            include_historical_metadata: self.include_historical_metadata,
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use insta::assert_json_snapshot;

    use super::*;

    #[derive(Debug, Serialize)]
    struct RequestSnap<Q, B> {
        method: String,
        endpoint: String,
        headers: HashMap<String, String>,
        query: Option<Q>,
        body: Option<B>,
    }

    // fn to_text<R: Request>(base_url: impl ToString, req: R) -> String {
    //     let url = base_url.to_string() + &req.endpoint();
    //     format!(
    //         "{} {} HTTP/1.1\r\n{}\r\n\r\n{}",
    //         R::HTTP_METHOD,
    //         base_url.to_string() + &req.endpoint() + ,
    //         req.headers()
    //             .iter()
    //             .map(|(k, v)| format!("{}: {}", k, v.to_str().unwrap()))
    //             .collect::<Vec<_>>()
    //             .join("\r\n"),
    //         req.body()
    //             .map(|b| serde_json::to_string(&b).unwrap())
    //             .unwrap_or("".to_string())
    //     )
    // }

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
            body: request.body(),
        }
    }

    #[test]
    fn test_list_shares_request() {
        let request = ListSharesRequest::builder()
            .max_results(10)
            .page_token("foo".to_string())
            .build();
        assert_json_snapshot!(into_delta_sharing_client_request(request));
    }
}
