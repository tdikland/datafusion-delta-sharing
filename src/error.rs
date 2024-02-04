//! Delta Sharing errors types

use std::{
    error::Error,
    fmt::{Display, Formatter},
};

use arrow_schema::ArrowError;
use datafusion::error::DataFusionError;

#[derive(Debug)]
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

    pub fn kind(&self) -> DeltaSharingErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        self.message.as_ref()
    }

    pub fn profile(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::ProfileError, message)
    }

    pub fn client(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::ClientError, message)
    }

    pub fn server(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::ServerError, message)
    }

    pub fn parse_response(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::ParseResponseError, message)
    }

    pub fn parse_securable(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::ParseSecurableError, message)
    }

    pub fn request(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::RequestError, message)
    }

    pub fn other(message: impl Into<String>) -> Self {
        Self::new(DeltaSharingErrorKind::Other, message)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeltaSharingErrorKind {
    /// Error related to Delta Sharing profile
    ProfileError,
    /// Error related to parsing shared object names
    ParseSecurableError,
    ParseResponseError,
    /// Error related to the the Delta Sharing Client
    ClientError,
    /// Error related to the Delta Sharing Server
    ServerError,
    RequestError,
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
