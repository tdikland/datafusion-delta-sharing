//! Delta Sharing REST client

use super::profile;

mod client;
mod error;
pub mod request;
pub mod response;

pub use client::RestClient;
pub use error::RestClientError;
