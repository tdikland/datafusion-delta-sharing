use std::fmt::{Display, Formatter};

use serde::Deserialize;

use crate::securable::{Schema, Share, Table};

use super::action::{File, Metadata, Protocol};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    error_code: String,
    message: String,
}

impl ErrorResponse {
    pub fn error_code(&self) -> &str {
        &self.error_code
    }

    pub fn _message(&self) -> &str {
        &self.message
    }
}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.error_code, self.message)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSharesResponse {
    items: Vec<Share>,
    next_page_token: Option<String>,
}

impl ListSharesResponse {
    pub fn items(&self) -> &[Share] {
        &self.items
    }

    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
}

impl IntoIterator for ListSharesResponse {
    type Item = Share;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

#[derive(Debug, Deserialize)]
pub struct GetShareResponse {
    share: Share,
}

impl GetShareResponse {
    pub fn share(&self) -> &Share {
        &self.share
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSchemasResponse {
    items: Vec<Schema>,
    next_page_token: Option<String>,
}

impl ListSchemasResponse {
    pub fn items(&self) -> &[Schema] {
        &self.items
    }

    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
}

impl IntoIterator for ListSchemasResponse {
    type Item = Schema;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTablesResponse {
    items: Vec<Table>,
    next_page_token: Option<String>,
}

impl ListTablesResponse {
    pub fn items(&self) -> &[Table] {
        &self.items
    }

    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
}

impl IntoIterator for ListTablesResponse {
    type Item = Table;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

#[derive(Debug, Deserialize)]
pub enum ParquetResponse {
    #[serde(rename = "protocol")]
    Protocol(Protocol),
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    #[serde(rename = "file")]
    File(File),
}

impl ParquetResponse {
    pub fn _as_protocol(&self) -> Option<&Protocol> {
        match self {
            ParquetResponse::Protocol(p) => Some(p),
            _ => None,
        }
    }

    pub fn to_protocol(self) -> Option<Protocol> {
        match self {
            ParquetResponse::Protocol(p) => Some(p),
            _ => None,
        }
    }

    pub fn to_file(self) -> Option<File> {
        match self {
            ParquetResponse::File(f) => Some(f),
            _ => None,
        }
    }

    pub fn _as_metadata(&self) -> Option<&Metadata> {
        match self {
            ParquetResponse::Metadata(m) => Some(m),
            _ => None,
        }
    }

    pub fn to_metadata(self) -> Option<Metadata> {
        match self {
            ParquetResponse::Metadata(m) => Some(m),
            _ => None,
        }
    }

    pub fn _as_file(&self) -> Option<&File> {
        match self {
            ParquetResponse::File(f) => Some(f),
            _ => None,
        }
    }
}
