use reqwest::{Client, RequestBuilder};
use serde::{Deserialize, Serialize};

use crate::auth::Profile;

use super::{
    error::ClientError,
    request::{
        GetShareRequest, ListSchemasRequest, ListSharesRequest, ListTablesInSchemaRequest,
        ListTablesInShareRequest, Request, RequestBody,
    },
    response::{GetShareResponse, ListSchemasResponse, ListSharesResponse, ListTablesResponse},
};

pub struct DeltaSharingClient {
    inner: Client,
    profile: Profile,
}

impl DeltaSharingClient {
    pub fn new(profile: Profile) -> Self {
        Self {
            inner: Client::new(),
            profile,
        }
    }

    async fn send<R: Request>(&self, request: R) -> Result<R::Response, ClientError> {
        let url = format!("{}{}", self.profile.endpoint(), request.endpoint());
        let response = self
            .inner
            .request(R::HTTP_METHOD, &url)
            .headers(request.headers())
            .query(&request.query())
            .with_body(request.body())
            .with_auth(&self.profile)
            .send()
            .await
            .unwrap();

        // TODO
        response.json().await.unwrap()
    }

    pub async fn list_shares_paginated(
        &self,
        max_results: Option<u32>,
        page_token: Option<String>,
    ) -> Result<ListSharesResponse, ClientError> {
        let request = ListSharesRequest::builder()
            .maybe_max_results(max_results)
            .maybe_page_token(page_token)
            .build();
        self.send(request).await
    }

    pub async fn get_share(&self, share: String) -> Result<GetShareResponse, ClientError> {
        let request = GetShareRequest::builder().share_name(share).build();
        self.send(request).await
    }

    pub async fn list_schemas(
        &self,
        share: String,
        max_results: Option<u32>,
        page_token: Option<String>,
    ) -> Result<ListSchemasResponse, ClientError> {
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
        max_results: Option<u32>,
        page_token: Option<String>,
    ) -> Result<ListTablesResponse, ClientError> {
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
        max_results: Option<u32>,
        page_token: Option<String>,
    ) -> Result<ListTablesResponse, ClientError> {
        let request = ListTablesInShareRequest::builder()
            .share_name(share)
            .maybe_max_results(max_results)
            .maybe_page_token(page_token)
            .build();
        self.send(request).await
    }

    pub async fn query_table_version(&self) {
        todo!()
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

trait RequestBuilderExt: Sized {
    fn with_body<T: Serialize>(self, body: RequestBody<T>) -> Self;

    fn with_auth(self, auth: &Profile) -> Self;
}

impl RequestBuilderExt for RequestBuilder {
    fn with_body<T: Serialize>(self, body: RequestBody<T>) -> Self {
        todo!()
    }

    fn with_auth(self, auth: &Profile) -> Self {
        todo!()
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;

//     #[test]
//     fn pag() {}
// }
