//! Delta Sharing Client

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use reqwest::{Client, Method, Response, Url};
use serde_json::Deserializer;
use tracing::{debug, info, trace, warn};

use crate::{
    client::{
        action::File,
        response::{ErrorResponse, GetShareResponse, ParquetResponse},
    },
    delta_client::response::DeltaResponseLine,
    error::DeltaSharingError,
    profile::{DeltaSharingProfileExt, Profile},
    securable::{Schema, Share, Table},
};

const QUERY_PARAM_VERSION_TIMESTAMP: &str = "startingTimestamp";

pub mod response;

/// Delta Sharing client
#[derive(Debug, Clone)]
pub struct DeltaSharingDeltaClient {
    client: Client,
    profile: Profile,
}

impl DeltaSharingDeltaClient {
    /// Create a new Delta Sharing client
    pub fn new(profile: Profile) -> Self {
        Self {
            client: Client::new(),
            profile,
        }
    }

    /// Retrieve the profile of the client
    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    /// Retrieve the data of a table
    pub async fn get_table_data(
        &self,
        table: &Table,
        predicates: Option<String>,
        limit: Option<u32>,
    ) -> Result<Vec<DeltaResponseLine>, DeltaSharingError> {
        let url = url_for_table(self.profile.endpoint().clone(), table, Some("query"));
        info!("requesting: {}", url);
        let mut body: HashMap<String, String> = HashMap::new();

        let response = self
            .client
            .request(Method::POST, url)
            .header("delta-sharing-capabilities", "responseformat=delta")
            .json(&body)
            .authorize_with_profile(&self.profile)?
            .send()
            .await?;
        let status = response.status();

        if !status.is_success() {
            warn!("get table data status: {:?}", status);
            let err = response.json::<ErrorResponse>().await.unwrap();
            if status.is_client_error() {
                Err(DeltaSharingError::client(err.to_string()))
            } else {
                Err(DeltaSharingError::server(err.to_string()))
            }
        } else {
            let full = response.bytes().await?;

            let text = unsafe { String::from_utf8_unchecked(full.as_ref().to_vec()) };
            info!(text = %text, "full text");

            let mut lines = Deserializer::from_slice(&full).into_iter::<DeltaResponseLine>();

            let p = lines
                .next()
                .and_then(Result::ok)
                .ok_or(DeltaSharingError::parse_response("parsing protocol failed"))?;
            info!(protocol=?p, "parsed protocol");

            let m = lines
                .next()
                .and_then(Result::ok)
                .ok_or(DeltaSharingError::parse_response("parsing metadata failed"))?;
            info!(metadata=?m, "parsed metadata");

            let mut files = vec![p, m];
            for line in lines {
                info!(line=?line, "processing line");
                let file = line.ok();
                if let Some(f) = file {
                    files.push(f);
                }
            }

            Ok(files)
        }
    }

    async fn request(&self, method: Method, url: Url) -> Result<Response, DeltaSharingError> {
        self.client
            .request(method, url)
            .authorize_with_profile(&self.profile)
            .unwrap()
            .send()
            .await
            .map_err(Into::into)
    }
}

fn url_for_share(endpoint: Url, share: &Share, res: Option<&str>) -> Url {
    let mut url = endpoint;
    url.path_segments_mut()
        .expect("valid base")
        .pop_if_empty()
        .push("shares")
        .push(share.name());
    if let Some(r) = res {
        url.path_segments_mut().expect("valid base").push(r);
    }
    url
}

fn url_for_schema(endpoint: Url, schema: &Schema, res: Option<&str>) -> Url {
    let mut url = endpoint;
    url.path_segments_mut()
        .expect("valid base")
        .pop_if_empty()
        .extend(&["shares", schema.share_name(), "schemas", schema.name()]);
    if let Some(r) = res {
        url.path_segments_mut().expect("valid base").push(r);
    }
    url
}

fn url_for_table(endpoint: Url, table: &Table, res: Option<&str>) -> Url {
    let mut url = endpoint;
    url.path_segments_mut()
        .expect("valid base")
        .pop_if_empty()
        .extend(&[
            "shares",
            table.share_name(),
            "schemas",
            table.schema_name(),
            "tables",
            table.name(),
        ]);
    if let Some(r) = res {
        url.path_segments_mut().expect("valid base").push(r);
    }
    url
}

fn parse_table_version(response: &Response) -> Result<u64, DeltaSharingError> {
    response
        .headers()
        .get("Delta-Table-Version")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or(DeltaSharingError::parse_response("parsing version failed"))
}

#[cfg(test)]
mod test {
    use chrono::{TimeZone, Utc};
    use httpmock::MockServer;
    use serde_json::json;
    use tracing_test::traced_test;

    use crate::profile::ProfileType;

    use super::*;

    fn build_client() -> DeltaSharingDeltaClient {
        let profile_type = ProfileType::new_bearer_token("xx", None);
        let mock_server_url = "http://127.0.0.1:3000".parse::<Url>().unwrap();
        let profile: Profile = Profile::from_profile_type(1, mock_server_url, profile_type);
        DeltaSharingDeltaClient::new(profile)
    }

    // #[traced_test]
    // #[tokio::test]
    // async fn get_table_version() {
    //     let server = MockServer::start();
    //     let mock = server.mock(|when, then| {
    //         when.method("GET")
    //             .path("/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/version")
    //             .query_param("startingTimestamp", "2022-01-01T00:00:00Z")
    //             .header_exists("authorization");
    //         then.status(200)
    //             .header("delta-table-version", "123")
    //             .header("content-type", "application/json")
    //             .header("charset", "utf-8")
    //             .body("");
    //     });
    //     let client = build_sharing_client(&server);
    //     let table = Table::new(
    //         "vaccine_share",
    //         "acme_vaccine_data",
    //         "vaccine_patients",
    //         None,
    //         None,
    //     );
    //     let starting_timestamp = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();

    //     let result = client
    //         .get_table_version(&table, Some(starting_timestamp))
    //         .await
    //         .unwrap();

    //     mock.assert();
    //     assert_eq!(result, 123);
    // }

    // #[traced_test]
    // #[tokio::test]
    // async fn get_table_metadata() {
    //     let server = MockServer::start();
    //     let mock = server.mock(|when, then| {
    //         when.method("GET")
    //             .path("/shares/vaccine_share/schemas/acme_vaccine_data/tables/vaccine_patients/metadata")
    //             .header_exists("authorization");
    //         then.status(200)
    //             .header("content-type", "application/x-ndjson")
    //             .header("charset", "utf-8")
    //             .header("delta-table-version", "123")
    //             .body_from_file("./src/client/resources/get_table_metadata.ndjson");
    //     });
    //     let client = build_sharing_client(&server);
    //     let table = Table::new(
    //         "vaccine_share",
    //         "acme_vaccine_data",
    //         "vaccine_patients",
    //         None,
    //         None,
    //     );

    //     let (protocol, metadata) = client.get_table_metadata(&table).await.unwrap();

    //     mock.assert();
    //     assert_eq!(protocol.min_reader_version(), 1);
    //     assert_eq!(metadata.id(), "table_id");
    //     assert_eq!(metadata.format().provider(), "parquet");
    //     assert_eq!(metadata.schema_string(), "schema_as_string");
    //     assert_eq!(metadata.partition_columns(), &["date"]);
    // }

    // table = Table(table_name, "lin_dvsharing_bugbash_share_20231113", "regular_schema")

    #[traced_test]
    #[tokio::test]
    async fn get_table_data() {
        let client = build_client();
        let table = Table::new("tim", "esther", "smoes", None, None);

        let result = client
            .get_table_data(&table, Some("{\"foo\": \"bar\"}".into()), Some(100))
            .await
            .unwrap();

        println!("{result:?}");
        assert!(false);
    }
}
