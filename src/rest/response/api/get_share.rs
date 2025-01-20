use async_trait::async_trait;
use reqwest::Response;
use serde::Deserialize;

use crate::model;

use super::util::has_json_content_type;
use super::{FromResponse, ParseResponseError};

#[derive(Debug)]
pub struct GetShareResponse {
    pub share: model::Share,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetShareBody {
    share: model::Share,
}

#[async_trait]
impl FromResponse for GetShareResponse {
    type Error = ParseResponseError;

    async fn parse(res: Response) -> Result<Self, Self::Error> {
        if !has_json_content_type(res.headers()) {
            return Err(ParseResponseError::MissingJsonContentType);
        }

        let bytes = res
            .bytes()
            .await
            .map_err(|e| ParseResponseError::BodyError {
                source: Box::new(e),
            })?;

        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        let body: GetShareBody =
            serde_path_to_error::deserialize(&mut deserializer).map_err(|e| {
                ParseResponseError::DecodeBody {
                    path: e.path().to_string(),
                    source: Box::new(e.into_inner()),
                }
            })?;

        Ok(GetShareResponse { share: body.share })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use http::{header::CONTENT_TYPE, Response as HttpResponse};

    #[tokio::test]
    async fn example() {
        let body = r#"{"share":{"name":"share1","id":"1"}}"#;
        let response = HttpResponse::builder()
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .unwrap();

        let result = GetShareResponse::parse(response.try_into().unwrap())
            .await
            .unwrap();
        assert_eq!(result.share.id(), Some("1"));
        assert_eq!(result.share.name(), "share1");
    }
}
