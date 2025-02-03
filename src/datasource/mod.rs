//! Datafusion TableProvider for Delta Sharing
//!
//! The easiest way to register a shared Delta Lake table with DataFusion is to directly create a
//! [`DeltaSharingTable`] using a connection string and register it with the [`SessionContext`]. The
//! connection string is formatted as follows: `<path_to_profile_file>#<share_name>.<schema_name>.
//! <table_name>`.
//!
//! Example:
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # use datafusion_delta_sharing::error::DeltaSharingError;
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
//! ctx.sql("select * from demo").await?.show().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(()) };
//! # Ok(()) }
//! ```

mod error;
mod scan;
mod schema;
mod table;

pub use error::DataSourceError;
pub use table::DeltaSharingTable;
