//! Datafusion TableProvider for Delta Sharing
//!
//! Example:
//!
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

mod exec;
mod reader;
mod scan;
mod table;
mod format;

mod s;

pub use table::{DeltaSharingTable, DeltaSharingTableBuilder};
