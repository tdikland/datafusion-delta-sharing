use std::{fs::File, path::Path};

use reqwest::Url;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Profile {
    share_credentials_version: u32,
    endpoint: String,
    bearer_token: Option<String>,
}

#[derive(Debug, Clone)]
pub enum DeltaSharingProfile {
    BearerToken(BearerTokenProfile),
}

impl DeltaSharingProfile {
    pub fn new(endpoint: String, token: String) -> Self {
        let profile = BearerTokenProfile { endpoint, token };
        Self::BearerToken(profile)
    }

    pub fn endpoint(&self) -> &str {
        match self {
            DeltaSharingProfile::BearerToken(p) => p.endpoint(),
        }
    }

    pub fn token(&self) -> &str {
        match self {
            DeltaSharingProfile::BearerToken(p) => p.token(),
        }
    }

    pub fn url(&self) -> Url {
        Url::parse(self.endpoint()).unwrap()
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        let path = path.as_ref();
        let file = File::open(path).unwrap();
        let profile: Profile = serde_json::from_reader(file).unwrap();
        Self::BearerToken(BearerTokenProfile {
            endpoint: profile.endpoint,
            token: profile.bearer_token.unwrap(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct BearerTokenProfile {
    endpoint: String,
    token: String,
}

impl BearerTokenProfile {
    fn endpoint(&self) -> &str {
        self.endpoint.as_ref()
    }

    fn token(&self) -> &str {
        self.token.as_ref()
    }
}
