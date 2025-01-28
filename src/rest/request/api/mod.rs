use super::error::RequestBuilderError;
use super::IntoRequest;

pub mod get_share;
pub mod list_schemas;
pub mod list_shares;
pub mod list_tables_in_schema;
pub mod list_tables_in_share;
pub mod query_table_changes;
pub mod query_table_data;
pub mod query_table_metadata;
pub mod query_table_version;

pub use get_share::GetShareRequest;
pub use list_shares::ListSharesRequest;
