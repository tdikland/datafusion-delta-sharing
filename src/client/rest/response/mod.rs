use async_trait::async_trait;
use reqwest::Response;

mod api;
mod error;
mod line;
mod util;

pub use api::*;
pub use error::{ErrorResponse, ResponseError};

const DELTA_TABLE_VERSION_HEADERNAME: &str = "Delta-Table-Version";

#[async_trait]
pub trait FromResponse: Sized {
    type Error;

    async fn parse(res: Response) -> Result<Self, Self::Error>;
}
