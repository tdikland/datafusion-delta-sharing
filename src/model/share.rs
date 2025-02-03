//! The share model

use core::fmt;

use bon::Builder;
use serde::{Deserialize, Serialize};

/// Share model
///
/// A share is a logical grouping to share with recipients. A share can be
/// shared with one or multiple recipients. A recipient can access all
/// resources in a share. A share may contain multiple schemas.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Builder)]
#[serde(rename_all = "camelCase")]
#[builder(on(String, into))]
pub struct ShareInfo {
    name: String,
    id: Option<String>,
}

impl ShareInfo {
    /// Retrieve the name from `self`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use datafusion_delta_sharing::model::ShareInfo;
    ///
    /// let share = ShareInfo::builder().name("my-share").build();
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
    /// use datafusion_delta_sharing::model::ShareInfo;
    ///
    /// let share = ShareInfo::builder()
    ///     .name("my-share")
    ///     .id("my-share-id")
    ///     .build();
    /// assert_eq!(share.id(), Some("my-share-id"));
    /// ```
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
}

impl fmt::Display for ShareInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn builder() {
        let share = ShareInfo::builder().name("share").id("1").build();
        assert_eq!(share.name(), "share");
        assert_eq!(share.id(), Some("1"));
    }

    #[test]
    fn display_share() {
        let share = ShareInfo::builder().name("share").id("1").build();
        assert_eq!(format!("{}", share), "share");
    }
}
