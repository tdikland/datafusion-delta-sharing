//! Delta Sharing server response types.

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use delta_kernel::actions::{Add, Metadata, Protocol};
use serde::{Deserialize, Serialize};

use crate::securable::{Schema, Share, Table};

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
    pub fn message(&self) -> &str {
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
    pub share: Share,
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

// /// Delta Sharing server response lines for successful `get_table_metadata`,
// /// `get_table_data` and `get_table_changes` requests (in parquet format).
// #[derive(Debug, Deserialize)]
// pub enum ParquetResponse {
//     /// Protocol response
//     #[serde(rename = "protocol")]
//     Protocol(Protocol),
//     /// Metadata response
//     #[serde(rename = "metaData")]
//     Metadata(Metadata),
//     /// File response
//     #[serde(rename = "file")]
//     File(File),
// }

// impl ParquetResponse {
//     /// Retrieve the protocol of the response
//     pub fn to_protocol(self) -> Option<Protocol> {
//         match self {
//             ParquetResponse::Protocol(p) => Some(p),
//             _ => None,
//         }
//     }

//     /// Retrieve the metadata of the response
//     pub fn to_file(self) -> Option<File> {
//         match self {
//             ParquetResponse::File(f) => Some(f),
//             _ => None,
//         }
//     }

//     /// Retrieve the metadata of the response
//     pub fn to_metadata(self) -> Option<Metadata> {
//         match self {
//             ParquetResponse::Metadata(m) => Some(m),
//             _ => None,
//         }
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TableAction {
    Parquet(ParquetAction),
    Delta(DeltaAction),
}

impl TableAction {
    pub fn is_parquet(&self) -> bool {
        matches!(self, TableAction::Parquet(_))
    }

    pub fn is_delta(&self) -> bool {
        matches!(self, TableAction::Delta(_))
    }

    pub fn as_parquet(&self) -> Option<&ParquetAction> {
        match self {
            TableAction::Parquet(p) => Some(p),
            _ => None,
        }
    }

    pub fn as_delta(&self) -> Option<&DeltaAction> {
        match self {
            TableAction::Delta(d) => Some(d),
            _ => None,
        }
    }

    pub fn to_parquet(self) -> Option<ParquetAction> {
        match self {
            TableAction::Parquet(p) => Some(p),
            _ => None,
        }
    }

    pub fn to_delta(self) -> Option<DeltaAction> {
        match self {
            TableAction::Delta(d) => Some(d),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ParquetAction {
    Protocol(ParquetProtocolAction),
    #[serde(rename = "metaData")]
    Metadata(ParquetMetadataAction),
    File(ParquetFileAction),
}

impl ParquetAction {
    pub fn is_protocol(&self) -> bool {
        matches!(self, ParquetAction::Protocol(_))
    }

    pub fn is_metadata(&self) -> bool {
        matches!(self, ParquetAction::Metadata(_))
    }

    pub fn is_file(&self) -> bool {
        matches!(self, ParquetAction::File(_))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParquetProtocolAction {
    min_reader_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParquetMetadataAction {
    id: String,
    name: Option<String>,
    description: Option<String>,
    // format: ParquetResponseFormat,
    schema_string: String,
    partition_columns: Vec<String>,
    #[serde(default)]
    configuration: HashMap<String, Option<String>>,
    version: Option<u64>,
    size: Option<u64>,
    num_files: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParquetFileAction {
    url: String,
    id: String,
    partition_values: HashMap<String, Option<String>>,
    size: u64,
    stats: Option<String>,
    version: Option<u64>,
    timestamp: Option<u64>,
    expiration_timestamp: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeltaAction {
    Protocol(DeltaProtocolAction),
    #[serde(rename = "metaData")]
    Metadata(DeltaMetadataAction),
    File(DeltaFileAction),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaProtocolAction {
    delta_protocol: Protocol,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaMetadataAction {
    delta_metadata: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeltaSingleAction {
    Add(Add),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaFileAction {
    id: String,
    deletion_vector_field_id: Option<String>,
    version: Option<u64>,
    timestamp: Option<u64>,
    expiration_timestamp: Option<u64>,
    delta_single_action: DeltaSingleAction,
}

pub struct WrappedResponse {
    version: u64,
    
}
