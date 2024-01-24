use std::time::Duration;

use reqwest::{header::USER_AGENT, Method};
use reqwest_middleware::ClientWithMiddleware;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use reqwest_tracing::TracingMiddleware;

use self::{
    action::{File, Metadata, Protocol},
    securable::{Share, Table},
};

pub mod action;
pub mod profile;
pub mod response;
pub mod rest;
pub mod securable;

pub enum ClientError {
    Other(String),
}

pub struct DeltaSharingClient {
    client: ClientWithMiddleware,
    endpoint: String,
    token: String,
}

impl DeltaSharingClient {
    pub fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Self {
        let inner_client = reqwest::ClientBuilder::new()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("valid client configuration");

        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let client = reqwest_middleware::ClientBuilder::new(inner_client)
            .with(TracingMiddleware::default())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        Self {
            client,
            endpoint: endpoint.into(),
            token: token.into(),
        }
    }

    fn request(&self, method: Method, url: &str) -> reqwest_middleware::RequestBuilder {
        self.client
            .request(method, url)
            .bearer_auth(&self.token)
            .header(USER_AGENT, "datafusion-delta-sharing/0.0.1")
    }

    // async fn list_shares_page(&self) -> Result<ListSharesResponse, ClientError> {
    //     let url =
    //     Ok(self
    //         .client
    //         .get(format!("{}/shares", self.endpoint))
    //         .bearer_auth(&self.token)
    //         .send()
    //         .await
    //         .map_err(|e| ClientError::Other(e.to_string()))?
    //         .json()
    //         .await
    //         .map_err(|e| ClientError::Other(e.to_string()))?)
    // }

    pub async fn list_shares(&self) -> Result<Vec<Share>, ClientError> {
        let mut shares = vec![];
        let mut done = false;
        while !done {
            let response = self
                .client
                .get(format!("{}/shares", self.endpoint))
                .bearer_auth(&self.token)
                .send()
                .await
                .map_err(|e| ClientError::Other(e.to_string()))?
                .json::<ListSharesResponse>()
                .await
                .map_err(|e| ClientError::Other(e.to_string()))?;

            shares.extend(response.items);
            done = response.next_page_token.is_none();
        }

        Ok(shares)
    }

    pub async fn get_all_tables(&self, share: &str) -> Vec<String> {
        todo!();
    }

    pub async fn get_table_metadata(&self, table: &Table) -> Metadata {
        let response = self
            .client
            .get(format!(
                "{}/shares/{}/schemas/{}/tables/{}/metadata",
                self.endpoint,
                table.share_name(),
                table.schema_name(),
                table.name()
            ))
            .bearer_auth(&self.token)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<ParquetResponse>(line).unwrap())
            .collect::<Vec<_>>();

        for r in response {
            match r {
                ParquetResponse::Metadata(m) => return m,
                _ => (),
            }
        }

        unreachable!()
    }

    pub async fn get_table_files(&self, table: &Table) -> Vec<File> {
        let response = self
            .client
            .post(format!(
                "{}/shares/{}/schemas/{}/tables/{}/query",
                self.endpoint,
                table.share_name(),
                table.schema_name(),
                table.name()
            ))
            .bearer_auth(&self.token)
            .json(&serde_json::json!({
              "predicateHints": [],
            }))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<ParquetResponse>(line).unwrap())
            .collect::<Vec<_>>();

        println!("{response:?}");

        for res in response.iter() {
            match res {
                ParquetResponse::Protocol(p) => println!("FOUND PROTOCOL!\n{p:?}"),
                ParquetResponse::Metadata(m) => println!("FOUND METADATA!\n{m:?}"),
                ParquetResponse::File(f) => println!("FOUND FILE!\n{f:?}"),
            }
        }

        let mut files = vec![];
        for res in response.into_iter() {
            match res {
                ParquetResponse::File(f) => files.push(f),
                _ => (),
            }
        }

        files
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum ParquetResponse {
    #[serde(rename = "protocol")]
    Protocol(Protocol),
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    #[serde(rename = "file")]
    File(File),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ListSharesResponse {
    items: Vec<Share>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[cfg(test)]
mod test {
    use test::securable::Schema;

    use super::*;

    #[tokio::test]
    async fn it_works() {
        let endpoint = std::env::var("SHARING_ENDPOINT").unwrap();
        let token = std::env::var("SHARING_TOKEN").unwrap();
        let client = DeltaSharingClient::new(endpoint, token);

        // let shares = client._list_shares().await;

        // println!("{shares:?}");
        // assert!(true);
    }

    // #[tokio::test]
    // async fn it_works_md() {
    //     let endpoint = std::env::var("SHARING_ENDPOINT").unwrap();
    //     let token = std::env::var("SHARING_TOKEN").unwrap();
    //     let client = DeltaSharingClient::new(endpoint, token);

    //     let share = Share::new("tim_dikland_share", None);
    //     let schema = Schema::new(share, "sse", None);
    //     let table = Table::new(schema, "customers", None, "my-storage-path", None);
    //     let shares = client.get_table_metadata(&table).await;

    //     println!("{shares:?}");
    //     assert!(true);
    // }

    // #[tokio::test]
    // async fn it_works_files() {
    //     let endpoint = std::env::var("SHARING_ENDPOINT").unwrap();
    //     let token = std::env::var("SHARING_TOKEN").unwrap();
    //     let client = DeltaSharingClient::new(endpoint, token);

    //     let share = Share::new("tim_dikland_share", None);
    //     let schema = Schema::new(share, "sse", None);
    //     let table = Table::new(schema, "config", None, "my-storage-path", None);
    //     let shares = client.get_table_files(&table).await;

    //     println!("{shares:?}");
    //     assert!(false);
    // }
}
