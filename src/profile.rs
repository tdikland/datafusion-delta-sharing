//! Delta Sharing profile.
//!
//! The Delta Sharing profile is used to authenticate with a Delta Sharing
//! server. It contains the endpoint and the token to authenticate with the
//! server. Usually the profile is stored in a file and can be loaded from
//! there.
//!
//! Currently only bearer token authentication is supported.
//!
//! # Example
//! ```no_run,rust
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use datafusion_delta_sharing::Profile;
//!
//! let profile = Profile::try_from_path("./path/to/profile.json")?;
//! # Ok(()) }
//! ```
use std::{fmt::Formatter, fs::File, path::Path};

use chrono::{DateTime, Utc};
use reqwest::RequestBuilder;
use serde::Deserialize;
use url::Url;

use crate::error::DeltaSharingError;

/// The structure of a Delta Sharing profile file.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProfileFile {
    share_credentials_version: u32,
    endpoint: String,
    bearer_token: Option<String>,
    expiration_time: Option<DateTime<Utc>>,
}

/// Delta Sharing profile.
///
/// The Delta Sharing profile is used to connect with a Delta Sharing server.
/// The profile contains the endpoint and authentication information to make
/// a succesful connection.
#[derive(Debug, Clone)]
pub struct Profile {
    share_credentials_version: u32,
    endpoint: Url,
    profile_type: ProfileType,
}

impl Profile {
    /// Try to create a new Delta Sharing profile from a profile file.
    ///
    /// # Example
    /// ```no_run,rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use datafusion_delta_sharing::Profile;
    ///
    /// let profile = Profile::try_from_path("./path/to/profile.json")?;
    /// # Ok(()) }
    /// ```
    pub fn try_from_path<P: AsRef<Path>>(path: P) -> Result<Self, DeltaSharingError> {
        let file = File::open(path.as_ref()).map_err(|e| {
            DeltaSharingError::profile(format!(
                "Failed to open profile file at {}: {}",
                path.as_ref().display(),
                e
            ))
        })?;
        let profile_file = serde_json::from_reader::<_, ProfileFile>(file).map_err(|e| {
            DeltaSharingError::profile(format!(
                "Failed to parse profile file at {}: {}",
                path.as_ref().display(),
                e
            ))
        })?;

        let version = profile_file.share_credentials_version;
        let endpoint = profile_file.endpoint.parse::<Url>().map_err(|e| {
            DeltaSharingError::profile(format!("Failed to parse endpoint URL in profile: {}", e))
        })?;
        if version == 1 {
            if let Some(token) = profile_file.bearer_token {
                let profile_type =
                    ProfileType::new_bearer_token(token, profile_file.expiration_time);
                Ok(Self::from_profile_type(version, endpoint, profile_type))
            } else {
                Err(DeltaSharingError::profile(
                    "Bearer token is missing in profile file",
                ))
            }
        } else {
            Err(DeltaSharingError::profile(format!(
                "Unsupported share credentials version: {}",
                version
            )))
        }
    }

    /// Create a new Delta Sharing profile with a profile type.
    ///
    /// # Example
    /// ```rust
    /// use datafusion_delta_sharing::{Profile, profile::ProfileType};
    /// use url::Url;
    ///
    /// let version = 1;
    /// let endpoint = Url::parse("https://sharing.delta.io/delta-sharing/").unwrap();
    /// let profile_type = ProfileType::new_bearer_token("foo", None);
    ///
    /// let profile = Profile::from_profile_type(version, endpoint.clone(), profile_type);
    /// assert_eq!(profile.share_credentials_version(), 1);
    /// assert_eq!(profile.endpoint(), &endpoint);
    /// assert!(profile.is_bearer_token());
    /// ```
    pub fn from_profile_type(
        share_credentials_version: u32,
        endpoint: Url,
        profile_type: ProfileType,
    ) -> Self {
        Self {
            share_credentials_version,
            endpoint,
            profile_type,
        }
    }

    /// Retrieve the share credentials version from the profile.
    ///
    /// # Example
    /// ```rust
    /// use datafusion_delta_sharing::{Profile, profile::ProfileType};
    /// use url::Url;
    ///
    /// let version = 1;
    /// let endpoint = "https://sharing.delta.io/delta-sharing/".parse::<Url>().unwrap();
    /// let profile_type = ProfileType::new_bearer_token("token", None);
    /// let profile = Profile::from_profile_type(1, endpoint, profile_type);
    ///
    /// assert_eq!(profile.share_credentials_version(), 1);
    /// ````
    pub fn share_credentials_version(&self) -> u32 {
        self.share_credentials_version
    }

    /// Retrieve the endpoint from the profile.
    ///
    /// # Example
    /// ```rust
    /// use datafusion_delta_sharing::{Profile, profile::ProfileType};
    /// use url::Url;
    ///
    /// let version = 1;
    /// let endpoint = "https://sharing.delta.io/delta-sharing/".parse::<Url>().unwrap();
    /// let profile_type = ProfileType::new_bearer_token("token", None);
    /// let profile = Profile::from_profile_type(1, endpoint.clone(), profile_type);
    ///
    /// assert_eq!(profile.endpoint(), &endpoint);
    /// ```
    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Create a new Delta Sharing profile using a bearer token.
    ///
    /// # Example
    /// ```
    /// use datafusion_delta_sharing::Profile;
    ///
    /// let profile = Profile::new_bearer_token(1, "https://sharing.delta.io/delta-sharing/", "token", None);
    /// assert!(profile.is_bearer_token());
    /// ```
    pub fn new_bearer_token(
        version: u32,
        endpoint: impl Into<String>,
        bearer_token: impl Into<String>,
        expiration_time: Option<DateTime<Utc>>,
    ) -> Self {
        let profile_type = ProfileType::new_bearer_token(bearer_token.into(), expiration_time);
        Self {
            share_credentials_version: version,
            endpoint: Url::parse(&endpoint.into()).unwrap(),
            profile_type,
        }
    }

    /// Check if the profile is a bearer token profile.
    ///
    /// # Example
    /// ```rust
    /// use datafusion_delta_sharing::{Profile, profile::ProfileType};
    /// use url::Url;
    ///
    /// let version = 1;
    /// let endpoint = "https://sharing.delta.io/delta-sharing/".parse::<Url>().unwrap();
    /// let profile_type = ProfileType::new_bearer_token("token", None);
    /// let profile = Profile::from_profile_type(1, endpoint, profile_type);
    ///
    /// assert!(profile.is_bearer_token());
    /// ```
    pub fn is_bearer_token(&self) -> bool {
        self.profile_type.is_bearer_token()
    }
}

/// Profile type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ProfileType {
    BearerToken(BearerToken),
}

impl ProfileType {
    /// Create a new bearer token profile type.
    ///
    /// # Example
    /// ```rust
    /// use datafusion_delta_sharing::profile::ProfileType;
    ///
    /// let profile_type = ProfileType::new_bearer_token("token", None);
    /// assert!(profile_type.is_bearer_token());
    /// ```
    pub fn new_bearer_token(
        token: impl Into<String>,
        expiration_time: Option<DateTime<Utc>>,
    ) -> Self {
        Self::BearerToken(BearerToken::new(token.into(), expiration_time))
    }

    /// Check if the profile type is a bearer token.
    ///
    /// # Example
    /// ```rust
    /// use datafusion_delta_sharing::profile::ProfileType;
    ///
    /// let profile_type = ProfileType::new_bearer_token("token", None);
    /// assert!(profile_type.is_bearer_token());
    /// ```
    pub fn is_bearer_token(&self) -> bool {
        matches!(self, Self::BearerToken(_))
    }
}

/// Bearer token profile type.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BearerToken {
    token: String,
    expiration_time: Option<DateTime<Utc>>,
}

impl BearerToken {
    /// Create a new bearer token profile type.
    ///
    /// # Example
    /// ```rust
    /// use datafusion_delta_sharing::profile::BearerToken;
    ///
    /// let bearer_token = BearerToken::new("token", None);
    /// assert_eq!(bearer_token.token(), "token");
    /// assert_eq!(bearer_token.expiration_time(), None);
    /// ```
    pub fn new(token: impl Into<String>, expiration_time: Option<DateTime<Utc>>) -> Self {
        Self {
            token: token.into(),
            expiration_time,
        }
    }

    /// Retrieve the bearer token from the profile.
    ///
    /// # Example
    /// ```rust
    /// use datafusion_delta_sharing::profile::BearerToken;
    ///
    /// let bearer_token = BearerToken::new("token", None);
    ///
    /// assert_eq!(bearer_token.token(), "token");
    /// ```
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Retrieve the expiration time of the bearer token.
    ///
    /// # Example
    /// ```rust
    /// use datafusion_delta_sharing::profile::BearerToken;
    /// use chrono::{TimeZone, Utc};
    ///
    /// let expiration_time = Utc.with_ymd_and_hms(2021, 7, 14, 0, 0, 0).unwrap();
    /// let bearer_token = BearerToken::new("token", Some(expiration_time));
    /// assert_eq!(bearer_token.expiration_time(), Some(expiration_time));
    /// ```
    pub fn expiration_time(&self) -> Option<DateTime<Utc>> {
        self.expiration_time
    }

    /// Check if the bearer token has expired.
    ///
    /// If the expiration time is not set, the token is considered to be valid
    /// indefinitely.
    ///
    /// # Example
    /// ```rust
    /// use std::{thread, time::Duration};
    /// use datafusion_delta_sharing::profile::BearerToken;
    /// use chrono::Utc;
    ///
    /// let bearer_token = BearerToken::new("token", None);
    /// assert!(!bearer_token.has_expired());
    ///
    /// let expiration_time = Utc::now() + Duration::from_secs(1);
    /// let bearer_token = BearerToken::new("token", Some(expiration_time));
    /// assert!(!bearer_token.has_expired());
    ///
    /// thread::sleep(std::time::Duration::from_secs(2));
    /// assert!(bearer_token.has_expired());
    /// ```
    pub fn has_expired(&self) -> bool {
        if let Some(expiration_time) = self.expiration_time {
            expiration_time < Utc::now()
        } else {
            false
        }
    }
}

impl std::fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BearerTokenFile")
            .field("token", &"********")
            .field("expiration_time", &self.expiration_time)
            .finish()
    }
}

pub(crate) trait DeltaSharingProfileExt
where
    Self: Sized,
{
    fn authorize_with_profile(self, profile: &Profile) -> Result<Self, DeltaSharingError>;
}

impl DeltaSharingProfileExt for RequestBuilder {
    fn authorize_with_profile(self, profile: &Profile) -> Result<Self, DeltaSharingError> {
        let authorized_request_builder = match &profile.profile_type {
            ProfileType::BearerToken(b) => {
                if b.has_expired() {
                    return Err(DeltaSharingError::profile(
                        "Bearer token in profile has expired",
                    ));
                }
                self.bearer_auth(&b.token)
            }
        };
        Ok(authorized_request_builder)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn profile_from_path_v1_bearer_token() {
        let mut profile_file = tempfile::NamedTempFile::new().unwrap();
        let val = json!({
            "shareCredentialsVersion": 1,
            "endpoint": "https://sharing.delta.io/delta-sharing/",
            "bearerToken": "foo-token",
            "expirationTime": "2021-11-14T00:12:29.0Z"
        });
        serde_json::to_writer(&mut profile_file, &val).unwrap();
        let profile_path = profile_file.path();

        let profile = Profile::try_from_path(profile_path).unwrap();
        assert_eq!(profile.share_credentials_version(), 1);
        assert_eq!(
            profile.endpoint().to_string(),
            "https://sharing.delta.io/delta-sharing/"
        );
    }

    #[test]
    fn profile_from_path_missing_file() {
        let profile = Profile::try_from_path("/path/to/missing.profile");
        assert!(profile.is_err());
        assert!(profile
            .unwrap_err()
            .to_string()
            .starts_with("[PROFILE_ERROR] Failed to open profile file at "));
    }

    #[test]
    fn profile_from_path_malformed_file() {
        let mut profile_file = tempfile::NamedTempFile::new().unwrap();
        serde_json::to_writer(&mut profile_file, &json!({"malformed": "true"})).unwrap();
        let profile_path = profile_file.path();

        let profile = Profile::try_from_path(profile_path);
        assert!(profile.is_err());
        assert!(profile
            .unwrap_err()
            .to_string()
            .starts_with("[PROFILE_ERROR] Failed to parse profile file at"));
    }

    #[test]
    fn profile_from_path_malformed_endpoint() {
        let mut profile_file = tempfile::NamedTempFile::new().unwrap();
        let val = json!({
            "shareCredentialsVersion": 1,
            "endpoint": "malformed-url",
        });
        serde_json::to_writer(&mut profile_file, &val).unwrap();
        let profile_path = profile_file.path();

        let profile = Profile::try_from_path(profile_path);
        assert!(profile.is_err());
        assert!(profile
            .unwrap_err()
            .to_string()
            .starts_with("[PROFILE_ERROR] Failed to parse endpoint URL in profile"));
    }

    #[test]
    fn fail_without_bearer_token() {
        let mut profile_file = tempfile::NamedTempFile::new().unwrap();
        let val = json!({
            "shareCredentialsVersion": 1,
            "endpoint": "https://sharing.delta.io/delta-sharing/",
            "expirationTime": "2021-11-14T00:12:29.0Z"
        });
        serde_json::to_writer(&mut profile_file, &val).unwrap();
        let profile_path = profile_file.path();

        let profile = Profile::try_from_path(profile_path);
        assert!(profile.is_err());

        let error = profile.unwrap_err();
        assert_eq!(
            error.to_string(),
            "[PROFILE_ERROR] Bearer token is missing in profile file"
        );
    }

    #[test]
    fn debug_bearer_token_profile_type() {
        let profile = ProfileType::new_bearer_token("token", None);

        assert_eq!(
            format!("{:?}", profile),
            r#"BearerToken(BearerTokenFile { token: "********", expiration_time: None })"#
        );
    }

    #[test]
    fn authenticate_request_with_bearer_token() {
        let profile = Profile::new_bearer_token(
            1,
            "https://sharing.delta.io/delta-sharing/",
            "test-token",
            None,
        );
        let request = reqwest::Client::new().get("https://example.com");
        let request = request
            .authorize_with_profile(&profile)
            .unwrap()
            .build()
            .unwrap();

        let headers = request.headers();
        let auth_header = headers.get("Authorization").unwrap().to_str().unwrap();
        assert_eq!(auth_header, "Bearer test-token");
    }

    #[test]
    fn authenticate_request_with_expired_token() {
        let expiration_time = Utc::now() - chrono::Duration::days(1);
        let profile = Profile::new_bearer_token(
            1,
            "https://sharing.delta.io/delta-sharing/",
            "test-token",
            Some(expiration_time),
        );
        let request_builder = reqwest::Client::new()
            .get("https://example.com")
            .authorize_with_profile(&profile);

        assert!(request_builder.is_err());
        assert_eq!(
            request_builder.unwrap_err().to_string(),
            "[PROFILE_ERROR] Bearer token in profile has expired"
        );
    }
}
