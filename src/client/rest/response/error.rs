use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    error_code: String,
    message: String,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub enum ResponseError {
    MissingJsonContentType,
    MissingNdJsonContentType,
    MissingRequiredHeader { header_name: String },
    InvalidHeader { header_name: String, err: String },
    BodyError { source: BoxError },
    DecodeBody { path: String, source: BoxError },
    MissingProtocol,
    MissingMetadata,
    UnexpectedEndOfStream,
    UnexpectedWrapperObject { expected: String },
}

impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResponseError::MissingJsonContentType => {
                write!(f, "missing 'application/json' content type")
            }
            ResponseError::MissingNdJsonContentType => {
                write!(f, "missing 'application/x-ndjson' content type")
            }
            ResponseError::MissingRequiredHeader { header_name } => {
                write!(f, "missing required header: {}", header_name)
            }
            ResponseError::InvalidHeader { header_name, err } => {
                write!(f, "invalid header '{}': {}", header_name, err)
            }
            ResponseError::BodyError { source } => {
                write!(f, "error reading response body: {}", source)
            }
            ResponseError::DecodeBody { path, source } => {
                write!(f, "error decoding response body at '{}': {}", path, source)
            }
            ResponseError::UnexpectedEndOfStream => {
                write!(f, "unexpected end of stream")
            }
            ResponseError::UnexpectedWrapperObject { expected } => {
                write!(f, "unexpected wrapper object: expected '{}'", expected)
            }
            ResponseError::MissingProtocol => {
                write!(f, "missing protocol response line")
            }
            ResponseError::MissingMetadata => {
                write!(f, "missing metadata response line")
            }
        }
    }
}

impl std::error::Error for ResponseError {}
