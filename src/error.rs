//! Error type

use thiserror::Error;

use crate::{client::ClientError, datasource::DataSourceError};

/// Delta Sharing error type
#[derive(Debug, Error)]
pub enum DeltaSharingError {
    /// Delta Sharing client error
    #[error(transparent)]
    ClientError(#[from] ClientError),
    /// Delta Sharing datasource error
    #[error(transparent)]
    DataSourceError(#[from] DataSourceError),
}
