use std::{
    error::Error,
    fmt::{Display, Formatter},
};

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
    ParseSecurableError,
    ParseResponseError,
    ClientError,
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
            Self::ParseSecurableError => write!(f, "ParseSecurableError"),
            Self::ParseResponseError => write!(f, "ParseResponseError"),
            Self::RequestError => write!(f, "RequestError"),
            Self::ClientError => write!(f, "ClientError"),
            Self::ServerError => write!(f, "ServerError"),
            Self::Other => write!(f, "Other"),
        }
    }
}

impl Error for DeltaSharingError {}

impl From<reqwest::Error> for DeltaSharingError {
    fn from(e: reqwest::Error) -> Self {
        if e.is_decode() {
            return Self::parse_response(e.to_string());
        }
        Self::request(e.to_string())
    }
}
