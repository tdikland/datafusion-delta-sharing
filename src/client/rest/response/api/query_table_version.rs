use async_trait::async_trait;
use reqwest::Response;

use crate::model::TableVersion;

use super::util::extract_delta_table_version;
use super::{FromResponse, ParseResponseError};

#[derive(Debug)]
pub struct QueryTableVersionResponse {
    pub version: TableVersion,
}

#[async_trait]
impl FromResponse for QueryTableVersionResponse {
    type Error = ParseResponseError;

    async fn parse(res: Response) -> Result<Self, Self::Error> {
        let table_version = extract_delta_table_version(res.headers())?;
        Ok(QueryTableVersionResponse {
            version: TableVersion(table_version),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use http::Response as HttpResponse;

    #[tokio::test]
    async fn example() {
        let response = HttpResponse::builder()
            .header("Delta-Table-Version", "3")
            .body("")
            .unwrap();

        let parsed = QueryTableVersionResponse::parse(response.try_into().unwrap())
            .await
            .unwrap();
        assert_eq!(parsed.version.0, 3);
    }
}
