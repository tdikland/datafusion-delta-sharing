use http::StatusCode;

use super::{
    request::RequestError,
    response::{ErrorResponse, ResponseError},
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub enum RestClientError {
    RequestError(RequestError),
    HttpClientError(BoxError),
    MissingDeltaTableVersionHeader,
    InvalidDeltaTableVersionHeader(String),
    DecodeErrorResponse(String),
    ErrorResponse {
        status: http::StatusCode,
        body: ErrorResponse,
    },
    ParseResponse(ResponseError),
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

impl From<ResponseError> for RestClientError {
    fn from(err: ResponseError) -> Self {
        tracing::error!(err=?err, msg=%err, "Failed to parse response");
        Self::ParseResponse(err)
    }
}

impl From<RequestError> for RestClientError {
    fn from(err: RequestError) -> Self {
        tracing::error!(err=?err, msg=%err, "Failed to build request");
        Self::RequestError(err)
    }
}
