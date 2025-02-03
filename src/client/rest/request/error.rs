use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RequestError {
    #[error("Invalid URI: {0}")]
    InvalidUri(String),
    #[error("Failed to serialize request body: {0}")]
    SerializeBody(String),
    #[error("Invalid HTTP request: {0}")]
    HttpError(String),
}

impl RequestError {
    pub fn invalid_uri(uri: String) -> Self {
        RequestError::InvalidUri(uri)
    }
}

impl From<http::Error> for RequestError {
    fn from(e: http::Error) -> Self {
        RequestError::HttpError(e.to_string())
    }
}
