//! Delta Sharing datasource errors

use datafusion::error::DataFusionError;
use thiserror::Error;

use crate::client::ClientError;

/// Errors that can occur in the Delta Sharing datasource
#[derive(Debug, Error)]
pub enum DataSourceError {
    /// Invalid connection string
    #[error("Invalid connection string: {0}")]
    ConnectionString(String),
    /// Error loading the profile
    #[error("Error loading profile: {0}")]
    Profile(String),
    /// Error with the client
    #[error("Error with the client: {0}")]
    Client(#[from] ClientError),
    /// Invalid identifiers
    #[error("Invalid identifiers: {0}")]
    ParseTableSchema(String),
    /// InvalidSchemaProjection
    #[error("Invalid schema projection: {0}")]
    InvalidSchemaProjection(String),
}

impl From<DataSourceError> for DataFusionError {
    fn from(e: DataSourceError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}
