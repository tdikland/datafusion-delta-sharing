use reqwest::Url;

#[derive(Debug, Clone)]
struct Profile {
    share_credentials_version: u32,
    enpoint: String,
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
}

#[derive(Debug, Clone)]
struct BearerTokenProfile {
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
