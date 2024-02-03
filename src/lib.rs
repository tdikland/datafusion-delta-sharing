//! This create provides the integration between Delta Sharing and DataFusion.
//!
//! Delta Sharing is an open protocol for securely sharing large datasets. It
//! is a REST protocol that securely provides access to a part of a cloud
//! dataset stored on popular cloud storage systems like S3, ADLS, and GCS.
//!
//! An example of the integration is shown below:
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # async {
//! use std::sync::Arc;
//! use datafusion::prelude::*;
//!
//! use datafusion_delta_sharing::DeltaSharingProfile;
//! use datafusion_delta_sharing::datasource::DeltaSharingTableBuilder;
//! use datafusion_delta_sharing::securable::Table;
//!
//! let ctx = SessionContext::new();
//!
//! let profile = DeltaSharingProfile::from_path("./path/to/profile.share");
//! let table_fqn: Table = "my-share.my-schema.my-table".parse()?;
//!
//! let table = DeltaSharingTableBuilder::new()
//!     .with_profile(profile)
//!     .with_table(table_fqn)
//!     .build()
//!     .await?;
//!
//! ctx.register_table("demo", Arc::new(table))?;
//! let data = ctx.sql("select * from demo").await?.collect().await?;
//! # }
//! # Ok(()) }
//! ```

#![allow(missing_docs)]

pub mod catalog;
mod client;
mod datasource;
pub mod error;
mod profile;
mod securable;

pub use datasource::{DeltaSharingTable, DeltaSharingTableBuilder};
pub use error::{DeltaSharingError, DeltaSharingErrorKind};
pub use profile::Profile;
pub use securable::{Schema, Share, Table};
