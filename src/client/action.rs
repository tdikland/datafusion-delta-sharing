//! Basic types for describing table data and metadata

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Representation of the table protocol.
///
/// Protocol versioning will allow servers to exclude older clients that are
/// missing features required to correctly interpret their response if the
/// Delta Sharing Protocol evolves in the future. The protocol version will be
/// increased whenever non-backwards-compatible changes are made to the
/// protocol. When a client is running an unsupported protocol version, it
/// should show an error message instructing the user to upgrade to a newer
/// version of their client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    /// The minimum version of the protocol that the client must support.
    min_reader_version: u32,
}

impl Default for Protocol {
    fn default() -> Self {
        Self {
            min_reader_version: Self::CURRENT,
        }
    }
}

impl Protocol {
    pub const CURRENT: u32 = 1;

    /// Retrieve the minimum version of the protocol that the client must
    /// implement to read this table.
    pub fn min_reader_version(&self) -> u32 {
        self.min_reader_version
    }
}

/// Representation of the table format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Format {
    /// The format of the data files backing the shared table.
    provider: String,
    options: Option<HashMap<String, String>>,
}

impl Format {
    pub fn provider(&self) -> &str {
        self.provider.as_ref()
    }

    pub fn options(&self) -> Option<&HashMap<String, String>> {
        self.options.as_ref()
    }
}

impl Default for Format {
    fn default() -> Self {
        Self {
            provider: String::from("parquet"),
            options: None,
        }
    }
}

/// Representation of the table metadata.
///
/// The metadata of a table contains all the information required to correctly
/// interpret the data files of the table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: Format,
    schema_string: String,
    partition_columns: Vec<String>,
    #[serde(default)]
    configuration: HashMap<String, String>,
    version: Option<String>,
    size: Option<u64>,
    num_files: Option<u64>,
}

impl Metadata {
    /// Retrieve the unique table identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Retrieve the table name provided by the user.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Retrieve the table description provided by the user.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Retrieve the specification of the table format.
    pub fn format(&self) -> &Format {
        &self.format
    }

    /// Retrieve the schema of the table, serialized as a string.
    pub fn schema_string(&self) -> &str {
        &self.schema_string
    }

    /// Retrieve an array of column names that are used to partition the table.
    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    /// Retrieve a map containing configuration options for the table.
    pub fn configuration(&self) -> &HashMap<String, String> {
        &self.configuration
    }

    /// Retrieve the version of the table this metadata corresponds to.
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Retrieve the size of the table in bytes.
    pub fn size(&self) -> Option<u64> {
        self.size
    }

    /// Retrieve the number of files in the table.
    pub fn num_files(&self) -> Option<u64> {
        self.num_files
    }
}

/// Representation of data that is part of a table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct File {
    url: String,
    id: String,
    partition_values: HashMap<String, Option<String>>,
    size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expiration_timestamp: Option<u64>,
}

impl File {
    /// An HTTPS url that a client can use to directly read the data file.
    pub fn url(&self) -> &str {
        self.url.as_ref()
    }

    /// A mutable HTTPS url that a client can use to directly read the data file.
    pub fn url_mut(&mut self) -> &mut String {
        &mut self.url
    }

    /// A unique identifier for the data file in the table.
    pub fn id(&self) -> &str {
        self.id.as_ref()
    }

    /// A map from partition column to value for this file in the table.
    pub fn partition_values(&self) -> HashMap<String, String> {
        self.partition_values
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().unwrap_or_default()))
            .collect()
    }

    /// The size of this file in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Summary statistics about the data in this file.
    pub fn stats(&self) -> Option<&str> {
        self.stats.as_deref()
    }

    /// The table version associated with this file.
    pub fn version(&self) -> Option<u64> {
        self.version
    }

    /// The unix timestamp in milliseconds corresponding to the table version
    /// associated with this file.
    pub fn timestamp(&self) -> Option<u64> {
        self.timestamp
    }

    /// The unix timestamp in milliseconds corresponding to the expiration of
    /// the url associated with this file.
    pub fn expiration_timestamp(&self) -> Option<u64> {
        self.expiration_timestamp
    }
}

/// Representation of data that was added to a table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    /// An HTTPS url that a client can use to directly read the data file.
    pub url: String,
    /// A unique identifier for the data file in the table.
    pub id: String,
    /// A map from partition column to value for this file in the table.
    pub partition_values: HashMap<String, Option<String>>,
    /// The size of the file in bytes.
    pub size: u64,
    /// Summary statistics about the data in this file.
    pub stats: Option<String>,
    /// The table version associated with this file.
    pub version: u64,
    /// The unix timestamp in milliseconds corresponding to the table version
    /// associated with this file.
    pub timestamp: String,
    /// The unix timestamp in milliseconds corresponding to the expiration of
    /// the url associated with this file.
    pub expiration_timestamp: Option<String>,
}

/// Representation of a data that has changed in the table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Cdf {
    /// An HTTPS url that a client can use to directly read the data file.
    pub url: String,
    /// A unique identifier for the data file in the table.
    pub id: String,
    /// A map from partition column to value for this file in the table.
    pub partition_values: HashMap<String, Option<String>>,
    /// The size of the file in bytes.
    pub size: u64,
    /// Summary statistics about the data in this file.
    pub stats: Option<String>,
    /// The table version associated with this file.
    pub version: u64,
    /// The unix timestamp in milliseconds corresponding to the table version
    /// associated with this file.
    pub timestamp: String,
    /// The unix timestamp in milliseconds corresponding to the expiration of
    /// the url associated with this file.
    pub expiration_timestamp: Option<String>,
}

/// Representation of a data that has been removed from the table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    /// An HTTPS url that a client can use to directly read the data file.
    pub url: String,
    /// A unique identifier for the data file in the table.
    pub id: String,
    /// A map from partition column to value for this file in the table.
    pub partition_values: HashMap<String, Option<String>>,
    /// The size of the file in bytes.
    pub size: u64,
    /// Summary statistics about the data in this file.
    pub stats: Option<String>,
    /// The table version associated with this file.
    pub version: u64,
    /// The unix timestamp in milliseconds corresponding to the table version
    /// associated with this file.
    pub timestamp: String,
    /// The unix timestamp in milliseconds corresponding to the expiration of
    /// the url associated with this file.
    pub expiration_timestamp: Option<String>,
}
