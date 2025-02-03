use std::fmt;

use bon::Builder;
use serde::{Deserialize, Serialize};

/// Table model
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

impl fmt::Display for TableInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.share_name(),
            self.schema_name(),
            self.name()
        )
    }
}

/// Table version model
#[derive(Debug, Clone, Copy)]
pub struct TableVersionNumber(u64);

impl TableVersionNumber {
    /// Create a new [`TableVersionNumber`] with the given version number
    pub fn new(inner: u64) -> Self {
        Self(inner)
    }

    /// Returns the version number
    pub fn inner(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn builder() {
        let table = TableInfo::builder()
            .share("share")
            .schema("schema")
            .name("table")
            .share_id("share-id")
            .id("table-id")
            .build();
        assert_eq!(table.share_name(), "share");
        assert_eq!(table.schema_name(), "schema");
        assert_eq!(table.name(), "table");
        assert_eq!(table.share_id(), Some("share-id"));
        assert_eq!(table.id(), Some("table-id"));
    }

    #[test]
    fn display_table() {
        let table = TableInfo::builder()
            .share("share")
            .schema("schema")
            .name("table")
            .build();
        assert_eq!(format!("{}", table), "share.schema.table");
    }

    #[test]
    fn version_number() {
        let version = TableVersionNumber::new(1);
        assert_eq!(version.inner(), 1);
    }
}
