use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub enum DeltaSharingClientErrorKind {
    ConstructionError,
    TokenExpired,
    ClientError(String),
    ServerError(String),
    Other,
}

#[derive(Debug)]
pub struct DeltaSharingClientError {
    kind: DeltaSharingClientErrorKind,
    message: String,
}

impl DeltaSharingClientError {
    pub fn kind(&self) -> &DeltaSharingClientErrorKind {
        &self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn construction(msg: impl Into<String>) -> Self {
        Self {
            kind: DeltaSharingClientErrorKind::ConstructionError,
            message: msg.into(),
        }
    }

    pub fn token_expired(msg: impl Into<String>) -> Self {
        Self {
            kind: DeltaSharingClientErrorKind::TokenExpired,
            message: msg.into(),
        }
    }

    pub fn client(code: impl Into<String>, msg: impl Into<String>) -> Self {
        Self {
            kind: DeltaSharingClientErrorKind::ClientError(code.into()),
            message: msg.into(),
        }
    }

    pub fn server(code: impl Into<String>, msg: impl Into<String>) -> Self {
        Self {
            kind: DeltaSharingClientErrorKind::ServerError(code.into()),
            message: msg.into(),
        }
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Self {
            kind: DeltaSharingClientErrorKind::Other,
            message: msg.into(),
        }
    }
}

impl Display for DeltaSharingClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            DeltaSharingClientErrorKind::ConstructionError => {
                write!(f, "[CONSTRUCTION_ERROR] {}", self.message)
            }
            DeltaSharingClientErrorKind::TokenExpired => {
                write!(f, "[TOKEN_EXPIRED] {}", self.message)
            }
            DeltaSharingClientErrorKind::ClientError(code) => {
                write!(f, "[CLIENT_ERROR - {}] {}", code, self.message)
            }
            DeltaSharingClientErrorKind::ServerError(code) => {
                write!(f, "[SERVER_ERROR - {}] {}", code, self.message)
            }
            DeltaSharingClientErrorKind::Other => {
                write!(f, "[OTHER_ERROR] {}", self.message)
            }
        }
    }
}

impl Error for DeltaSharingClientError {}
