//! The share model.

use core::fmt;
use std::{convert::Infallible, str::FromStr};

use bon::Builder;
use serde::{Deserialize, Serialize};

/// The type of a share as defined in the Delta Sharing protocol.
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
    /// use datafusion_delta_sharing::model::Share;
    ///
    /// let share = Share::builder().name("my-share").build();
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
    /// use datafusion_delta_sharing::securable::Share;
    ///
    /// let share = Share::new("my-share", Some("my-share-id"));
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

impl FromStr for ShareInfo {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ShareInfo::builder().name(s).build())
    }
}

impl From<String> for ShareInfo {
    fn from(s: String) -> Self {
        ShareInfo::builder().name(s).build()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn display_share() {
        let share = ShareInfo::builder().name("share").id("1").build();
        assert_eq!(format!("{}", share), "share");
    }

    #[test]
    fn parse_share() {
        let share = "share".parse::<ShareInfo>().unwrap();
        // assert_eq!(share, Share::new("share", None));
        assert!(false)
    }
}
