use std::clone;

use async_trait::async_trait;
use reqwest::{Client, RequestBuilder};
use serde::Serialize;

use crate::auth::Profile;

use super::{
    error::RestClientError,
    request::{
        GetShareRequest, ListSchemasRequest, ListSharesRequest, ListTablesInSchemaRequest,
        ListTablesInShareRequest, QueryTableVersionRequest, Request,
    },
    response::{
        ErrorResponse, FromResponse, GetShareResponse, ListSchemasResponse, ListSharesResponse,
        ListTablesResponse, QueryTableVersionResponse,
    },
};

const CAPABILITIES_HEADER: &str = "delta-sharing-capabilities";
const CAPABILITIES: &str = "responseFormat=parquet";

static USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

pub struct RestClient {
    inner: Client,
    profile: Profile,
}

impl RestClient {
    pub fn new(profile: Profile) -> Self {
        let client = Client::builder().user_agent(USER_AGENT).build().unwrap();
        Self {
            inner: client,
            profile,
        }
    }

    pub(crate) async fn send<R: Request>(&self, request: R) -> Result<R::Response, RestClientError>
    where
        RestClientError: From<<<R as Request>::Response as FromResponse>::Error>,
    {
        let url = format!("{}{}", self.profile.endpoint(), request.endpoint());
        let response = self
            .inner
            .request(R::HTTP_METHOD, &url)
            .headers(request.headers())
            .header(CAPABILITIES_HEADER, CAPABILITIES)
            .query(&request.query())
            .with_body(request.body())
            .with_auth(&self.profile)
            .await
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            // let text = response.text().await.unwrap_or_default();
            // println!("status: {}", status);
            // println!("text: {}", text);
            // panic!("test");
            let error_response = response
                .json::<ErrorResponse>()
                .await
                .map_err(|e| RestClientError::DecodeErrorResponse(e.to_string()))?;

            Err(RestClientError::ErrorResponse {
                status,
                body: error_response,
            })
        } else {
            let parsed_response = R::Response::parse(response).await?;
            Ok(parsed_response)
        }
    }

    // pub async fn list_shares_paginated(
    //     &self,
    //     max_results: Option<String>,
    //     page_token: Option<String>,
    // ) -> Result<ListSharesResponse, RestClientError> {
    //     let request = ListSharesRequest::builder()
    //         .maybe_max_results(max_results)
    //         .maybe_page_token(page_token)
    //         .build();
    //     self.send(request).await
    // }

    pub async fn get_share(&self, share: String) -> Result<GetShareResponse, RestClientError> {
        let request = GetShareRequest::builder().share_name(share).build();
        self.send(request).await
    }

    pub async fn list_schemas(
        &self,
        share: String,
        max_results: Option<i32>,
        page_token: Option<String>,
    ) -> Result<ListSchemasResponse, RestClientError> {
        let request = ListSchemasRequest::builder()
            .share_name(share)
            .maybe_max_results(max_results)
            .maybe_page_token(page_token)
            .build();
        self.send(request).await
    }

    pub async fn list_tables_in_schema(
        &self,
        share: String,
        schema: String,
        max_results: Option<i32>,
        page_token: Option<String>,
    ) -> Result<ListTablesResponse, RestClientError> {
        let request = ListTablesInSchemaRequest::builder()
            .share_name(share)
            .schema_name(schema)
            .maybe_max_results(max_results)
            .maybe_page_token(page_token)
            .build();
        self.send(request).await
    }

    pub async fn list_tables_in_share(
        &self,
        share: String,
        max_results: Option<i32>,
        page_token: Option<String>,
    ) -> Result<ListTablesResponse, RestClientError> {
        let request = ListTablesInShareRequest::builder()
            .share_name(share)
            .maybe_max_results(max_results)
            .maybe_page_token(page_token)
            .build();
        self.send(request).await
    }

    pub async fn query_table_version(
        &self,
        share_name: String,
        schema_name: String,
        table_name: String,
        starting_timestamp: Option<String>,
    ) -> Result<QueryTableVersionResponse, RestClientError> {
        let request = QueryTableVersionRequest::builder()
            .share_name(share_name)
            .schema_name(schema_name)
            .table_name(table_name)
            .maybe_starting_timestamp(starting_timestamp)
            .build();
        self.send(request).await
    }

    pub async fn query_table_metadata(&self) {
        todo!()
    }

    pub async fn query_table_data(&self) -> Result<(), ()> {
        todo!()
    }

    pub async fn query_table_changes(&self) {
        todo!()
    }
}

#[async_trait]
trait RequestBuilderExt: Sized {
    fn with_body<T: Serialize>(self, body: Option<T>) -> Self;

    async fn with_auth(self, auth: &Profile) -> Self;
}

#[async_trait]
impl RequestBuilderExt for RequestBuilder {
    fn with_body<T: Serialize>(self, body: Option<T>) -> Self {
        match body {
            Some(b) => self.json(&b),
            None => self,
        }
    }

    // TODO: propagate result for expired token, failed to fetch new, etc.
    async fn with_auth(self, auth: &Profile) -> Self {
        let token = auth.get_token().await.unwrap();
        self.bearer_auth(token)
    }
}
