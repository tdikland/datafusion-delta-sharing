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
//! use datafusion_delta_sharing::DeltaSharingTable;
//!
//! let ctx = SessionContext::new();
//! let table = DeltaSharingTable::try_from_str("./path/to/profile.share#share.schema.table").await?;
//!
//! ctx.register_table("demo", Arc::new(table))?;
//! let data = ctx.sql("select * from demo").await?.collect().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(()) };
//! # Ok(()) }
//! ```

#![deny(missing_docs)]

pub mod catalog;
pub mod client;
pub mod datasource;
pub mod error;
pub mod profile;
pub mod securable;

pub use datasource::DeltaSharingTable;
pub use error::{DeltaSharingError, DeltaSharingErrorKind};
pub use profile::Profile;
