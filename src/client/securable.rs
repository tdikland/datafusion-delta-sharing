//! Basic types for describing shared objects

use std::fmt::Display;

use serde::{Deserialize, Serialize};

/// The type of a share as defined in the Delta Sharing protocol.
///
/// A share is a logical grouping to share with recipients. A share can be
/// shared with one or multiple recipients. A recipient can access all
/// resources in a share. A share may contain multiple schemas.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub struct Share {
    name: String,
    id: Option<String>,
}

impl Share {
    /// Create a new `Share` with the given `name` and `id`.
    pub fn new<S: Into<String>>(name: S, id: Option<S>) -> Self {
        Self {
            name: name.into(),
            id: id.map(Into::into),
        }
    }

    /// Retrieve the name from `self`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::Share;
    ///
    /// let share = Share::new("my-share", None);
    /// assert_eq!(share.name(), "my-share");
    /// ```
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// Retrieve the id from `self`.
    ///
    /// # Example
    ///  
    /// ```rust
    /// use delta_sharing_server::protocol::securable::Share;
    ///
    /// let share = Share::new("my-share", Some("my-share-id"));
    /// assert_eq!(share.id(), Some("my-share-id"));
    /// ```
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
}

impl Display for Share {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// The type of a schema as defined in the Delta Sharing protocol.
///
/// A schema is a logical grouping of tables. A schema may contain multiple
/// tables. A schema is defined within the context of a [`Share`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Schema {
    share: String,
    name: String,
}

impl Schema {
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
    /// use delta_sharing_server::protocol::securable::{Share, Schema};
    ///
    /// let schema = Schema::new("my-share", "my-schema");
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
    /// use delta_sharing_server::protocol::securable::{Schema};
    ///
    /// let schema = Schema::new("my-share", "my-schema");
    /// assert_eq!(schema.name(), "my-schema");
    /// ```
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.share_name(), self.name())
    }
}

/// The type of a table as defined in the Delta Sharing protocol.
///
/// A table is a Delta Lake table or a view on top of a Delta Lake table. A
/// table is defined within the context of a [`Schema`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash)]
#[serde(rename_all = "camelCase")]
pub struct Table {
    name: String,
    schema: String,
    share: String,
    share_id: Option<String>,
    id: Option<String>,
}

impl Table {
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
            share_id: share_id.map(Into::into),
            id: table_id.map(Into::into),
        }
    }

    /// Returns the name of the share associated with `self`
    ///
    /// # Example
    ///
    /// ```rust
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", None, "my-storage-path", None);
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
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", Some("my-share-id"));
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", None, "my-storage-path", None);
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
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", None, "my-storage-path", None);
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
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", None, "my-storage-path", None);
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
    /// use delta_sharing_server::protocol::securable::{Share, Schema, Table};
    ///
    /// let share = Share::new("my-share", None);
    /// let schema = Schema::new(share, "my-schema", None);
    /// let table = Table::new(schema, "my-table", Some("my-table-id"), "my-storage-path", None);
    /// assert_eq!(table.id(), Some("my-table-id"));
    /// ```
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
}

impl Display for Table {
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

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn display_share() {
//         let share = Share::new("share", Some("id"));
//         assert_eq!(format!("{}", share), "share");
//     }

//     #[test]
//     fn display_schema() {
//         let share = Share::new("share", Some("share_id"));
//         let schema = Schema::new(share, "schema", Some("schema_id"));
//         assert_eq!(format!("{}", schema), "share.schema");
//     }

//     #[test]
//     fn display_table() {
//         let share = Share::new("share", Some("share_id"));
//         let schema = Schema::new(share, "schema", Some("schema_id"));
//         let table = Table::new(
//             schema,
//             "table",
//             Some("table_id"),
//             "storage_path",
//             Some("format"),
//         );
//         assert_eq!(format!("{}", table), "share.schema.table");
//     }
// }
