use std::{fs::File, path::Path};

use reqwest::RequestBuilder;
use serde::Deserialize;

use crate::error::DeltaSharingError;

#[derive(Debug, Clone)]
pub enum DeltaSharingProfile {
    Bearer(BearerTokenProfile),
}

impl DeltaSharingProfile {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, DeltaSharingError> {
        let path = path.as_ref();
        let file = File::open(path).unwrap();
        let profile: Profile = serde_json::from_reader(file).unwrap();
        Ok(Self::Bearer(BearerTokenProfile {
            endpoint: profile.endpoint,
            token: profile.bearer_token.unwrap(),
        }))
    }

    pub fn new_bearer(endpoint: String, token: String) -> Self {
        Self::Bearer(BearerTokenProfile { endpoint, token })
    }
}

trait DeltaSharingProfileExt {
    fn profile_auth(self, profile: &DeltaSharingProfile) -> Self;
}

impl DeltaSharingProfileExt for RequestBuilder {
    fn profile_auth(self, profile: &DeltaSharingProfile) -> Self {
        match profile {
            DeltaSharingProfile::Bearer(b) => self.bearer_auth(&b.token),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BearerTokenProfile {
    endpoint: String,
    token: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Profile {
    share_credentials_version: u32,
    endpoint: String,
    bearer_token: Option<String>,
}
