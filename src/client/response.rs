use reqwest::StatusCode;
use serde::Deserialize;

use super::securable::{Schema, Share, Table};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    pub error_code: String,
    pub message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSharesPaginated {
    pub items: Vec<Share>,
    pub next_page_token: Option<String>,
}

impl IntoIterator for ListSharesPaginated {
    type Item = Share;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSchemasPaginated {
    pub items: Vec<Schema>,
    pub next_page_token: Option<String>,
}

impl IntoIterator for ListSchemasPaginated {
    type Item = Schema;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTablesPaginated {
    pub items: Vec<Table>,
    pub next_page_token: Option<String>,
}

impl IntoIterator for ListTablesPaginated {
    type Item = Table;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAllTablesPaginated {
    pub items: Vec<Table>,
    #[serde(default)]
    pub next_page_token: Option<String>,
}

impl IntoIterator for ListAllTablesPaginated {
    type Item = Table;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}
