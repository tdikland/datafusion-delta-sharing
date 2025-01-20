use http::StatusCode;

use super::response::{ErrorResponse, ParseResponseError};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub enum RestClientError {
    HttpClientError(BoxError),
    MissingDeltaTableVersionHeader,
    InvalidDeltaTableVersionHeader(String),
    DecodeErrorResponse(String),
    ErrorResponse {
        status: http::StatusCode,
        body: ErrorResponse,
    },
    ParseResponse(ParseResponseError),
}

impl RestClientError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::ErrorResponse { status, .. } if status == &StatusCode::NOT_FOUND)
    }
}

impl From<reqwest::Error> for RestClientError {
    fn from(err: reqwest::Error) -> Self {
        tracing::error!(err=?err, msg=%err, "HTTP error");
        RestClientError::HttpClientError(Box::new(err))
    }
}

impl From<ParseResponseError> for RestClientError {
    fn from(err: ParseResponseError) -> Self {
        tracing::error!(err=?err, msg=%err, "Failed to parse response");
        Self::ParseResponse(err)
    }
}
