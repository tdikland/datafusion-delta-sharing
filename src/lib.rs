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
//! use datafusion::prelude::*;
//! use std::sync::Arc;
//!
//! use datafusion_delta_sharing::DeltaSharingTable;
//!
//! let ctx = SessionContext::new();
//! let table =
//!     DeltaSharingTable::try_from_str("./path/to/profile.share#share.schema.table").await?;
//!
//! ctx.register_table("demo", Arc::new(table))?;
//! let data = ctx.sql("select * from demo").await?.collect().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(()) };
//! # Ok(()) }
//! ```

#![warn(missing_docs)]

mod catalog;
mod error;

pub mod client;
pub mod datasource;
pub mod model;

pub use catalog::{DeltaSharingCatalog, DeltaSharingCatalogList, DeltaSharingSchema};
pub use datasource::DeltaSharingTable;
pub use error::DeltaSharingError;
