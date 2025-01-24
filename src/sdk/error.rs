use core::fmt;
use std::convert::Infallible;

use crate::rest::error::RestClientError;

#[derive(Debug)]
pub enum ClientError {
    RestClientError(RestClientError),
    InvalidTableRef,
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

impl From<Infallible> for ClientError {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}

impl std::error::Error for ClientError {}
