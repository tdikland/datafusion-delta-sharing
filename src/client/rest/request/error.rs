use core::fmt;

#[derive(Debug)]
pub enum RequestBuilderError {
    UrlParseError(url::ParseError),
    HttpError(http::Error),
}

impl fmt::Display for RequestBuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl std::error::Error for RequestBuilderError {}

impl From<http::Error> for RequestBuilderError {
    fn from(e: http::Error) -> Self {
        RequestBuilderError::HttpError(e)
    }
}