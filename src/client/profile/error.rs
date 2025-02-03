//! Delta Sharing profile errors

use thiserror::Error;

/// Errors that can occur when working with the DeltaSharing profile.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProfileError {
    /// The profile file could not be opened.
    #[error("Could not open profile file at `{path}`: {error}")]
    OpenProfileFile {
        /// Path to the profile file.
        path: String,
        /// Error message.
        error: String,
    },
    /// The profile file could not be parsed.
    #[error("Could not parse profile file at path `{path}`: {error}")]
    ParseProfileFile {
        /// Path to the element that could not be deserialized.
        path: String,
        /// Error message.
        error: String,
    },
    /// Profile endpoint is not a valid URL.
    #[error("Invalid profile endpoint `{endpoint}`: {error}")]
    InvalidProfileEndpoint {
        /// Endpoint in the profile file.
        endpoint: String,
        /// Error message.
        error: String,
    },
    /// Unsupported share credentials version.
    #[error("Unsupported share credentials version: {version}")]
    UnsupportedCredentialVersion {
        /// Share credential version in the profile file.
        version: u32,
    },
    /// Incomplete profile.
    #[error("Profile is missing required field: {missing_field}")]
    IncompleteProfile {
        /// The profile file is missing a required field.
        missing_field: String,
    },
    /// The bearer token is expired and cannot be refreshed.
    #[error("Bearer token expired and cannot be refreshed")]
    BearerTokenExpired,
}

impl ProfileError {
    /// Create a new error for when the profile file could not be opened.
    pub fn open_file(path: String, error: String) -> Self {
        Self::OpenProfileFile { path, error }
    }

    /// Create a new error for when the profile file could not be parsed.
    pub fn parse_file(path: String, error: String) -> Self {
        Self::ParseProfileFile { path, error }
    }

    /// Create a new error for when the profile endpoint is not a valid URL.
    pub fn invalid_endpoint(endpoint: String, error: String) -> Self {
        Self::InvalidProfileEndpoint { endpoint, error }
    }

    /// Create a new error for when the share credentials version is unsupported.
    pub fn unsupported_version(version: u32) -> Self {
        Self::UnsupportedCredentialVersion { version }
    }

    /// Create a new error for when the profile is incomplete.
    pub fn incomplete_profile(missing: String) -> Self {
        Self::IncompleteProfile {
            missing_field: missing,
        }
    }
}
