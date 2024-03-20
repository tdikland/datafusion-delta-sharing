//! Datafusion TableProvider for Delta Sharing
//!
//! Example:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # use datafusion_delta_sharing::error::DeltaSharingError;
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
//! ctx.sql("select * from demo").await?.show().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(()) };
//! # Ok(()) }
//! ```

mod expr;
mod reader;
mod scan;
mod schema;
mod table;

mod kernel_scan;
mod mem_client;

pub use table::{DeltaSharingTable, DeltaSharingTableBuilder};
