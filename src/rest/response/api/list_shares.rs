use async_trait::async_trait;
use http_body::Body;
use http_body_util::BodyExt;
use reqwest::Response;
use serde::Deserialize;

use crate::model;
use crate::rest::response::FromHttpResponse;

use super::util::has_json_content_type;
use super::{FromResponse, ParseResponseError};

#[derive(Debug)]
pub struct ListSharesResponse {
    pub items: Vec<model::ShareInfo>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListSharesBody {
    items: Vec<model::ShareInfo>,
    next_page_token: Option<String>,
}

#[async_trait::async_trait]
impl FromResponse for ListSharesResponse {
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
        let body: ListSharesBody =
            serde_path_to_error::deserialize(&mut deserializer).map_err(|e| {
                ParseResponseError::DecodeBody {
                    path: e.path().to_string(),
                    source: Box::new(e.into_inner()),
                }
            })?;

        Ok(ListSharesResponse {
            items: body.items,
            next_page_token: body.next_page_token,
        })
    }
}

// #[async_trait]
// impl<T: Body + Send + Sync> FromHttpResponse<T> for ListSharesResponse {
//     type Error = ParseResponseError;

//     async fn from_response(response: http::Response<T>) -> Result<Self, Self::Error> {
//         let bytes = response
//             .into_body()
//             .collect()
//             .await
//             .map(|buf| buf.to_bytes())
//             .map_err(|e| ParseResponseError::MissingMetadata)?;

//         let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
//         let body: ListSharesBody =
//             serde_path_to_error::deserialize(&mut deserializer).map_err(|e| {
//                 ParseResponseError::DecodeBody {
//                     path: e.path().to_string(),
//                     source: Box::new(e.into_inner()),
//                 }
//             })?;

//         Ok(ListSharesResponse {
//             items: body.items,
//             next_page_token: body.next_page_token,
//         })
//     }
// }

// impl<T> FromHttpResponse<T> for ListSharesResponse
// where
//     T: http_body::Body + Send + Sync + 'static,
//     T::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
// {
//     type Error = ParseResponseError;

//     async fn from_response(response: http::Response<T>) -> Result<Self, Self::Error> {
//         let headers = response.headers();
//         if !has_json_content_type(headers) {
//             return Err(ParseResponseError::MissingJsonContentType);
//         }

//         let body = response.into_body();
//         let bytes = hyper::body::to_bytes(body)
//             .await
//             .map_err(|e| ParseResponseError::BodyError {
//                 source: Box::new(e),
//             })?;

//         let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
//         let body: ListSharesBody =
//             serde_path_to_error::deserialize(&mut deserializer).map_err(|e| {
//                 ParseResponseError::DecodeBody {
//                     path: e.path().to_string(),
//                     source: Box::new(e.into_inner()),
//                 }
//             })?;

//         Ok(ListSharesResponse {
//             items: body.items,
//             next_page_token: body.next_page_token,
//         })
//     }
// }

#[cfg(test)]
mod test {
    use super::*;

    use http::{header::CONTENT_TYPE, Response as HttpResponse};

    #[tokio::test]
    async fn example() {
        let body = r#"{"items":[{"name":"share1","id":"1"}],"nextPageToken":"token1"}"#;
        let response = HttpResponse::builder()
            .header(CONTENT_TYPE, "application/json; charset=utf-8")
            .body(body.to_string())
            .unwrap();

        let parsed = ListSharesResponse::parse(response.try_into().unwrap())
            .await
            .unwrap();

        assert_eq!(parsed.items.len(), 1);
        assert_eq!(parsed.items[0].name(), "share1");
        assert_eq!(parsed.items[0].id(), Some("1"));
        assert_eq!(parsed.next_page_token, Some("token1".to_string()));
    }
}
