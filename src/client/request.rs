use std::fmt;

use chrono::{DateTime, Utc};

use super::expr::Op;

/// The name of a share
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShareName {
    name: String,
}

impl ShareName {
    /// Construct a new share name.
    pub fn new(name: String) -> Self {
        Self { name }
    }

    /// Create a new share name.
    pub fn try_new(name: String) -> Result<Self, ParseNameError> {
        Ok(Self { name })
    }

    /// Get the name of the share.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl TryFrom<String> for ShareName {
    type Error = ParseNameError;

    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::try_new(v)
    }
}

impl TryFrom<&String> for ShareName {
    type Error = ParseNameError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::try_new(value.to_owned())
    }
}

impl TryFrom<&str> for ShareName {
    type Error = ParseNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_new(value.to_owned())
    }
}

impl fmt::Display for ShareName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// The name of a schema
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaName {
    share_name: String,
    name: String,
}

impl SchemaName {
    /// Construct a new schema name.
    pub fn new(share_name: String, schema_name: String) -> Self {
        Self {
            share_name,
            name: schema_name,
        }
    }

    /// Get the name of the share.
    pub fn share(&self) -> &str {
        &self.share_name
    }

    /// Get the name of the schema.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl TryFrom<String> for SchemaName {
    type Error = ParseNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() != 2 {
            return Err(ParseNameError::TableRef);
        }
        Ok(Self::new(parts[0].to_owned(), parts[1].to_owned()))
    }
}

impl TryFrom<&String> for SchemaName {
    type Error = ParseNameError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() != 2 {
            return Err(ParseNameError::TableRef);
        }
        Ok(Self::new(parts[0].to_owned(), parts[1].to_owned()))
    }
}

impl TryFrom<&str> for SchemaName {
    type Error = ParseNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // TODO: fix
        let parts: Vec<&str> = value.split('.').collect();
        Ok(Self::new(parts[0].to_owned(), parts[1].to_owned()))
    }
}

/// The name of a table
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableName {
    share_name: String,
    schema_name: String,
    table_name: String,
}

impl TableName {
    /// Get the name of the share.
    pub fn share(&self) -> &str {
        &self.share_name
    }

    /// Get the name of the schema.
    pub fn schema(&self) -> &str {
        &self.schema_name
    }

    /// Get the name of the table.
    pub fn name(&self) -> &str {
        &self.table_name
    }
}

impl TryFrom<String> for TableName {
    type Error = ParseNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() != 3 {
            return Err(ParseNameError::TableRef);
        }

        Ok(Self {
            share_name: parts[0].to_string(),
            schema_name: parts[1].to_string(),
            table_name: parts[2].to_string(),
        })
    }
}

impl TryFrom<&String> for TableName {
    type Error = ParseNameError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() != 3 {
            return Err(ParseNameError::TableRef);
        }

        Ok(Self {
            share_name: parts[0].to_string(),
            schema_name: parts[1].to_string(),
            table_name: parts[2].to_string(),
        })
    }
}

impl TryFrom<&str> for TableName {
    type Error = ParseNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() != 3 {
            return Err(ParseNameError::TableRef);
        }

        Ok(Self {
            share_name: parts[0].to_string(),
            schema_name: parts[1].to_string(),
            table_name: parts[2].to_string(),
        })
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.share_name, self.schema_name, self.table_name
        )
    }
}

/// Error that can occur when parsing a name.
#[derive(Debug)]
pub enum ParseNameError {
    /// The name is invalid.
    ShareRef,
    /// The name is invalid.
    SchemaRef,
    /// The name is invalid.
    TableRef,
}

#[derive(Debug, Default)]
pub struct QueryTableVersionOpts {
    timestamp: Option<DateTime<Utc>>,
}

impl QueryTableVersionOpts {
    pub fn new() -> Self {
        Self { timestamp: None }
    }

    pub fn with_starting_timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.timestamp
    }
}

/// Options for querying table data.
#[derive(Debug, Default)]
pub struct QueryTableDataOpts {
    predicate: Option<Op>,
    limit: Option<u32>,
    version: Option<QueryTableVersion>,
}

impl QueryTableDataOpts {
    /// Create a new set of options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the predicate for the query.
    pub fn with_predicate(mut self, predicate: Op) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Set the limit for the query.
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the version(range) that will be queried.
    pub fn with_version(mut self, version: QueryTableVersion) -> Self {
        self.version = Some(version);
        self
    }

    /// Get the predicate for the query.
    pub fn predicate(&self) -> Option<&Op> {
        self.predicate.as_ref()
    }

    /// Get the limit for the query.
    pub fn limit(&self) -> Option<u32> {
        self.limit
    }

    /// Get the version(range) that will be queried.
    pub fn version(&self) -> Option<&QueryTableVersion> {
        self.version.as_ref()
    }
}

#[derive(Debug)]
pub enum TableVersion {
    Number(u64),
    Timestamp(DateTime<Utc>),
}

impl TableVersion {
    pub fn number(version: u64) -> Self {
        Self::Number(version)
    }

    pub fn timestamp(ts: DateTime<Utc>) -> Self {
        Self::Timestamp(ts)
    }
}

#[derive(Debug)]
pub enum TableVersionRange {
    Version {
        start: u64,
        end: Option<u64>,
    },
    Timestamp {
        start: DateTime<Utc>,
        end: Option<DateTime<Utc>>,
    },
}

impl TableVersionRange {
    pub fn version(start: u64, end: Option<u64>) -> Self {
        Self::Version { start, end }
    }

    pub fn timestamp(start: DateTime<Utc>, end: Option<DateTime<Utc>>) -> Self {
        Self::Timestamp { start, end }
    }
}

#[derive(Debug)]
pub enum QueryTableVersion {
    PointInTime(TableVersion),
    Range(TableVersionRange),
}

impl QueryTableVersion {
    pub fn from_number(version_number: u64) -> Self {
        Self::PointInTime(TableVersion::Number(version_number))
    }

    pub fn from_timestamp(ts: DateTime<Utc>) -> Self {
        Self::PointInTime(TableVersion::Timestamp(ts))
    }

    pub fn from_number_range(start: u64, end: Option<u64>) -> Self {
        Self::Range(TableVersionRange::Version { start, end })
    }

    pub fn from_timestamp_range(start: DateTime<Utc>, end: Option<DateTime<Utc>>) -> Self {
        Self::Range(TableVersionRange::Timestamp { start, end })
    }
}
