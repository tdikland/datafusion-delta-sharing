use std::fmt::{Display, Formatter};

use serde::Deserialize;

use crate::securable::{Schema, Share, Table};

use super::action::{Add, Cdf, File, Metadata, Protocol, Remove};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    error_code: String,
    message: String,
}

impl ErrorResponse {
    pub fn _error_code(&self) -> &str {
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
pub struct ListSharesPaginated {
    items: Vec<Share>,
    next_page_token: Option<String>,
}

impl ListSharesPaginated {
    pub fn items(&self) -> &[Share] {
        &self.items
    }

    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
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
    items: Vec<Schema>,
    next_page_token: Option<String>,
}

impl ListSchemasPaginated {
    pub fn items(&self) -> &[Schema] {
        &self.items
    }

    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
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
    items: Vec<Table>,
    next_page_token: Option<String>,
}

impl ListTablesPaginated {
    pub fn items(&self) -> &[Table] {
        &self.items
    }

    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
}

impl IntoIterator for ListTablesPaginated {
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
    Add(Add),
    Cdc(Cdf),
    Remove(Remove),
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
