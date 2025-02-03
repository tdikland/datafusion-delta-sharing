//! Delta Sharing shared object types

use std::{fmt::Display, str::FromStr};

use bon::Builder;
use serde::{Deserialize, Serialize};

use crate::error::DeltaSharingError;

pub mod action;
mod schema;
mod share;
mod table;
// mod version;

pub use share::ShareInfo;
pub use table::TableVersionNumber;

/// The type of a schema as defined in the Delta Sharing protocol.
///
/// A schema is a logical grouping of tables. A schema may contain multiple
/// tables. A schema is defined within the context of a [`Share`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Builder)]
#[builder(on(String, into))]
#[serde(rename_all = "camelCase")]
pub struct SchemaInfo {
    share: String,
    name: String,
}

impl SchemaInfo {
    /// Create a new `Schema` with the given [`Share`], `name` and `id`.
    pub fn new(share_name: impl Into<String>, schema_name: impl Into<String>) -> Self {
        Self {
            share: share_name.into(),
            name: schema_name.into(),
        }
    }

    /// Returns the name of the share associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use datafusion_delta_sharing::model::SchemaInfo;
    ///
    /// let schema = SchemaInfo::builder()
    ///     .share("my-share")
    ///     .name("my-schema")
    ///     .build();
    /// assert_eq!(schema.share_name(), "my-share");
    /// ```
    pub fn share_name(&self) -> &str {
        self.share.as_ref()
    }

    /// Returns the name of `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use datafusion_delta_sharing::model::SchemaInfo;
    ///
    /// let schema = SchemaInfo::builder()
    ///     .share("my-share")
    ///     .name("my-schema")
    ///     .build();
    /// assert_eq!(schema.name(), "my-schema");
    /// ```
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

impl Display for SchemaInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.share_name(), self.name())
    }
}

impl FromStr for SchemaInfo {
    type Err = DeltaSharingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split('.').collect::<Vec<_>>();
        if parts.len() == 2 {
            Ok(SchemaInfo::new(parts[0], parts[1]))
        } else {
            Err(DeltaSharingError::parse_securable(
                "Schema must be of the form <share>.<schema>",
            ))
        }
    }
}

/// The type of a table as defined in the Delta Sharing protocol.
///
/// A table is a Delta Lake table or a view on top of a Delta Lake table. A
/// table is defined within the context of a [`Schema`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Builder)]
#[builder(on(String, into))]
#[serde(rename_all = "camelCase")]
pub struct TableInfo {
    name: String,
    schema: String,
    share: String,
    share_id: Option<String>,
    id: Option<String>,
}

impl TableInfo {
    /// Create a new `Table` with the given [`Schema`], `name`, `storage_path`,
    ///  `table_id` and `table_format`. Whenever the `table_id` is `None`, it
    /// will default to `DELTA`
    pub fn new(
        share_name: impl Into<String>,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
        share_id: Option<String>,
        table_id: Option<String>,
    ) -> Self {
        Self {
            name: table_name.into(),
            schema: schema_name.into(),
            share: share_name.into(),
            share_id,
            id: table_id,
        }
    }

    /// Returns the name of the share associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use datafusion_delta_sharing::model::TableInfo;
    ///
    /// let table = TableInfo::builder()
    ///     .share("my-share")
    ///     .schema("my-schema")
    ///     .name("my-table")
    ///     .build();
    /// assert_eq!(table.share_name(), "my-share");
    /// ```
    pub fn share_name(&self) -> &str {
        self.share.as_ref()
    }

    /// Returns the id of the share associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use datafusion_delta_sharing::model::TableInfo;
    ///
    /// let table = TableInfo::builder()
    ///     .share("my-share")
    ///     .schema("my-schema")
    ///     .name("my-table")
    ///     .share_id("my-share-id")
    ///     .build();
    /// assert_eq!(table.share_id(), Some("my-share-id"));
    /// ```
    pub fn share_id(&self) -> Option<&str> {
        self.share_id.as_deref()
    }

    /// Returns the name of the schema associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use datafusion_delta_sharing::model::TableInfo;
    ///
    /// let table = TableInfo::builder()
    ///     .share("my-share")
    ///     .schema("my-schema")
    ///     .name("my-table")
    ///     .build();
    /// assert_eq!(table.schema_name(), "my-schema");
    /// ```
    pub fn schema_name(&self) -> &str {
        self.schema.as_ref()
    }

    /// Returns the name of `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use datafusion_delta_sharing::model::TableInfo;
    ///
    /// let table = TableInfo::builder()
    ///     .share("my-share")
    ///     .schema("my-schema")
    ///     .name("my-table")
    ///     .build();
    /// assert_eq!(table.name(), "my-table");
    /// ```
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// Returns the id of `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use datafusion_delta_sharing::model::TableInfo;
    ///
    /// let table = TableInfo::builder()
    ///     .share("my-share")
    ///     .schema("my-schema")
    ///     .name("my-table")
    ///     .id("my-table-id")
    ///     .build();
    /// assert_eq!(table.id(), Some("my-table-id"));
    /// ```
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
}

impl Display for TableInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.share_name(),
            self.schema_name(),
            self.name()
        )
    }
}

impl FromStr for TableInfo {
    type Err = DeltaSharingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split('.').collect::<Vec<_>>();
        if parts.len() == 3 {
            Ok(TableInfo::new(parts[0], parts[1], parts[2], None, None))
        } else {
            Err(DeltaSharingError::parse_securable(
                "Table must be of the form <share>.<schema>.<table>",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_schema() {
        let schema = SchemaInfo::new("share", "schema");
        assert_eq!(format!("{}", schema), "share.schema");
    }

    #[test]
    fn parse_schema() {
        let schema = "share.schema".parse::<SchemaInfo>().unwrap();
        assert_eq!(schema, SchemaInfo::new("share", "schema"));
    }

    #[test]
    fn display_table() {
        let table = TableInfo::new("share", "schema", "table", None, None);
        assert_eq!(format!("{}", table), "share.schema.table");
    }

    #[test]
    fn parse_table() {
        let table = "share.schema.table".parse::<TableInfo>().unwrap();
        assert_eq!(
            table,
            TableInfo::new("share", "schema", "table", None, None)
        );
    }
}
