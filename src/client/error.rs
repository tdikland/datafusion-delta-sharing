use std::convert::Infallible;

use thiserror::Error;

use super::{request::ParseNameError, rest::RestClientError};

/// Client error.
#[derive(Debug, Error)]
pub enum ClientError {
    /// REST client error.
    #[error("REST client error")]
    RestClientError(RestClientError),
    /// Invalid table reference.
    #[error("invalid table reference")]
    InvalidTableRef,
}

impl From<RestClientError> for ClientError {
    fn from(e: RestClientError) -> Self {
        Self::RestClientError(e)
    }
}

impl From<ParseNameError> for ClientError {
    fn from(e: ParseNameError) -> Self {
        match e {
            ParseNameError::TableRef => ClientError::InvalidTableRef,
            _ => panic!("welp"),
        }
    }
}

impl From<Infallible> for ClientError {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}
