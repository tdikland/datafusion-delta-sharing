//! Delta Sharing protocol models

pub mod action;
mod schema;
mod share;
mod table;

pub use schema::SchemaInfo;
pub use share::ShareInfo;
pub use table::TableInfo;
pub use table::TableVersionNumber;
