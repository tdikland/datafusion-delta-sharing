use reqwest::Response;
use serde::Deserialize;

use crate::model;

use super::util::has_json_content_type;
use super::{FromResponse, ParseResponseError};

#[derive(Debug)]
pub struct ListSchemasResponse {
    pub items: Vec<model::Schema>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListSchemasBody {
    items: Vec<model::Schema>,
    next_page_token: Option<String>,
}

#[async_trait::async_trait]
impl FromResponse for ListSchemasResponse {
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
        let body: ListSchemasBody =
            serde_path_to_error::deserialize(&mut deserializer).map_err(|e| {
                ParseResponseError::DecodeBody {
                    path: e.path().to_string(),
                    source: Box::new(e.into_inner()),
                }
            })?;

        Ok(ListSchemasResponse {
            items: body.items,
            next_page_token: body.next_page_token,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use http::{header::CONTENT_TYPE, Response as HttpResponse};

    #[tokio::test]
    async fn example() {
        let body = r#"{"items":[{"name":"schema1","share":"share1"}],"nextPageToken":"token1"}"#;
        let response = HttpResponse::builder()
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .unwrap();

        let result = ListSchemasResponse::parse(response.try_into().unwrap())
            .await
            .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].name(), "schema1");
        assert_eq!(result.items[0].share_name(), "share1");
    }
}
