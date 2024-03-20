//! Delta Sharing server response types.

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::securable::{Schema, Share, Table};

use delta_kernel::actions::{Add, Metadata, Protocol};

/// Delta Sharing server response for failed requests.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    error_code: String,
    message: String,
}

impl ErrorResponse {
    /// Retrieve the error code of the response
    pub fn error_code(&self) -> &str {
        &self.error_code
    }

    /// Retrieve the message of the response
    pub fn _message(&self) -> &str {
        &self.message
    }
}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.error_code, self.message)
    }
}

/// Delta Sharing server response for successful `list_shares` requests.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSharesResponse {
    items: Vec<Share>,
    next_page_token: Option<String>,
}

impl ListSharesResponse {
    /// Retrieve the shares of the response
    pub fn items(&self) -> &[Share] {
        &self.items
    }

    /// Retrieve the next page token of the response
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

/// Delta Sharing server response for successful `get_share` requests.
#[derive(Debug, Deserialize)]
pub struct GetShareResponse {
    share: Share,
}

impl GetShareResponse {
    /// Retrieve the share of the response
    pub fn share(&self) -> &Share {
        &self.share
    }
}

/// Delta Sharing server response for successful `list_schemas` requests.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSchemasResponse {
    items: Vec<Schema>,
    next_page_token: Option<String>,
}

impl ListSchemasResponse {
    /// Retrieve the schemas of the response
    pub fn items(&self) -> &[Schema] {
        &self.items
    }

    /// Retrieve the next page token of the response
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

/// Delta Sharing server response for successful `list_tables_in_share` and
/// `list_tables_in_schema` requests.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTablesResponse {
    items: Vec<Table>,
    next_page_token: Option<String>,
}

impl ListTablesResponse {
    /// Retrieve the tables of the response
    pub fn items(&self) -> &[Table] {
        &self.items
    }

    /// Retrieve the next page token of the response
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeltaResponseLine {
    Protocol(ProtocolResponseLine),
    #[serde(rename = "metaData")]
    Metadata(MetadataResponseLine),
    File(FileResponseLine),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolResponseLine {
    pub delta_protocol: Protocol,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataResponseLine {
    pub delta_metadata: Metadata,
    version: Option<u64>,
    size: Option<u64>,
    num_files: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileResponseLine {
    id: String,
    deletion_vector_file_id: Option<String>,
    version: Option<u64>,
    timestamp: Option<u64>,
    expiration_timestamp: Option<u64>,
    pub delta_single_action: SingleAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SingleAction {
    Add(Add),
}
