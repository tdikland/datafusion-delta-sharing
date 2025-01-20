use core::fmt;

use crate::rest::error::RestClientError;

#[derive(Debug)]
pub enum ClientError {
    RestClientError(RestClientError),
}

impl From<RestClientError> for ClientError {
    fn from(e: RestClientError) -> Self {
        Self::RestClientError(e)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for ClientError {}
