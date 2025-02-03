use bytes::Bytes;
use http::Request;

use super::response::{self, FromResponse};

mod api;
mod error;

pub use api::*;
pub use error::RequestError;

pub trait IntoRequest {
    type Response: FromResponse;

    fn into_request(self) -> Result<Request<Bytes>, RequestError>;
}
