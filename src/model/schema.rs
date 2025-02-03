use std::fmt;

use bon::Builder;
use serde::{Deserialize, Serialize};

/// Schema model
///
/// A schema is a logical grouping of tables. A schema may contain multiple
/// tables. A schema is defined within the context of a [`super::ShareInfo`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Builder)]
#[builder(on(String, into))]
#[serde(rename_all = "camelCase")]
pub struct SchemaInfo {
    share: String,
    name: String,
}

impl SchemaInfo {
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
    /// assert_eq!(schema.share(), "my-share");
    /// ```
    pub fn share(&self) -> &str {
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

impl fmt::Display for SchemaInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.share(), self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder() {
        let schema = SchemaInfo::builder().share("share").name("schema").build();
        assert_eq!(schema.share(), "share");
        assert_eq!(schema.name(), "schema");
    }

    #[test]
    fn display_schema() {
        let schema = SchemaInfo::builder().share("share").name("schema").build();
        assert_eq!(format!("{}", schema), "share.schema");
    }
}
