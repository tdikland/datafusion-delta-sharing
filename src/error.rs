//! Delta Sharing errors types

use std::{
    error::Error,
    fmt::{Display, Formatter},
};

use arrow_schema::ArrowError;
use datafusion::error::DataFusionError;

/// Error type for Delta Sharing.
#[derive(Debug, Clone)]
pub struct DeltaSharingError {
    kind: DeltaSharingErrorKind,
    message: String,
}

impl DeltaSharingError {
    fn new(kind: DeltaSharingErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Retrieve the kind of the error
    pub fn kind(&self) -> DeltaSharingErrorKind {
        self.kind
    }

    /// Retrieve the message of the error
    pub fn message(&self) -> &str {
        self.message.as_ref()
    }

    /// Create a new profile error with a message
    pub fn profile(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::ProfileError, message)
    }

    /// Create a new sharing client error with a message
    pub fn client(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::ClientError, message)
    }

    /// Create a new sharing server error with a message
    pub fn server(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::ServerError, message)
    }

    /// Create a new parse response error with a message
    pub fn parse_response(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::ParseResponseError, message)
    }

    /// Create a new parse securable error with a message
    pub fn parse_securable(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::ParseSecurableError, message)
    }

    /// Create a new request error with a message
    pub fn request(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::RequestError, message)
    }

    /// Create a new other error with a message
    pub fn other(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::Other, message)
    }
}

/// Kind of Delta Sharing error
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeltaSharingErrorKind {
    /// Error related to Delta Sharing profile
    ProfileError,
    /// Error related to parsing shared object names
    ParseSecurableError,
    /// Error related to parsing the response from the server
    ParseResponseError,
    /// Error related to the the Delta Sharing Client
    ClientError,
    /// Error related to the Delta Sharing Server
    ServerError,
    /// Error related to the request
    RequestError,
    /// Other error
    Other,
}

impl Display for DeltaSharingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.kind, self.message)
    }
}

impl Display for DeltaSharingErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProfileError => write!(f, "PROFILE_ERROR"),
            Self::ParseSecurableError => write!(f, "ParseSecurableError"),
            Self::ParseResponseError => write!(f, "ParseResponseError"),
            Self::RequestError => write!(f, "RequestError"),
            Self::ClientError => write!(f, "SHARING_CLIENT_ERROR"),
            Self::ServerError => write!(f, "ServerError"),
            Self::Other => write!(f, "Other"),
        }
    }
}

impl Error for DeltaSharingError {}

impl From<reqwest::Error> for DeltaSharingError {
    fn from(value: reqwest::Error) -> Self {
        if value.is_decode() {
            return Self::parse_response(value.to_string());
        }
        Self::request(value.to_string())
    }
}

impl From<ArrowError> for DeltaSharingError {
    fn from(value: ArrowError) -> Self {
        Self::other(value.to_string())
    }
}

impl From<DeltaSharingError> for DataFusionError {
    fn from(e: DeltaSharingError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}
