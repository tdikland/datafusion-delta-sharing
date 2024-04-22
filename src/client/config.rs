use reqwest::header::{HeaderMap, USER_AGENT};

use crate::Profile;

pub struct DeltaSharingClientConfig {
    profile: Profile,
    headers: HeaderMap,
    capabilities: Capabilities,
}

impl DeltaSharingClientConfig {
    pub fn new(profile: Profile) -> Self {
        let mut headers = HeaderMap::new();
        headers.append(
            USER_AGENT,
            "datafusion-delta-sharing/0.1.0"
                .parse()
                .expect("valid user_agent"),
        );

        Self {
            profile,
            headers,
            capabilities: Default::default(),
        }
    }

    pub fn with_response_format(mut self, format: ResponseFormat) -> Self {
        self
    }
}

enum ResponseFormat {
    Parquet,
    Delta,
}

pub struct Capabilities {
    response_format: ResponseFormat,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            response_format: ResponseFormat::Parquet,
        }
    }
}
