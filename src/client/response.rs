use crate::model::{
    action::{
        parquet::Protocol,
        parquet::{File, Metadata},
    },
    TableVersionNumber,
};

#[derive(Debug)]
pub struct TableData {
    pub version: TableVersionNumber,
    pub format: TableDataFormat,
}

impl TableData {
    pub fn is_parquet_format(&self) -> bool {
        self.format.is_parquet()
    }

    pub fn into_parquet_files(self) -> Vec<File> {
        match self.format {
            TableDataFormat::Parquet { files, .. } => files,
        }
    }
}

#[derive(Debug)]
pub enum TableDataFormat {
    Parquet {
        protocol: Protocol,
        metadata: Metadata,
        files: Vec<File>,
    },
}

impl TableDataFormat {
    pub fn is_parquet(&self) -> bool {
        matches!(self, Self::Parquet { .. })
    }
}

/// Delta Sharing table metadata.
///
/// The precise metadata available depends on the table format. There is a lot of overlap between
/// the delta and parquet response format. [`TableMetadata`] as a struct abstracts over the
/// differences.
#[derive(Debug)]
pub struct TableMetadata {
    version: TableVersionNumber,
    format: TableMetadataFormat,
}

impl TableMetadata {
    /// Create a new [`TableMetadata`].
    pub fn new(version: TableVersionNumber, format: TableMetadataFormat) -> Self {
        Self { version, format }
    }

    /// Returns the table version number.
    pub fn table_version(&self) -> TableVersionNumber {
        self.version
    }

    /// Returns the table schema as a string.
    pub fn schema_string(&self) -> &str {
        match &self.format {
            TableMetadataFormat::Parquet { metadata, .. } => metadata.schema_string(),
        }
    }

    /// Returns the partition columns of the table.
    pub fn partition_columns(&self) -> &[String] {
        match &self.format {
            TableMetadataFormat::Parquet { metadata, .. } => metadata.partition_columns(),
        }
    }
}

#[derive(Debug)]
pub enum TableMetadataFormat {
    Parquet {
        protocol: Protocol,
        metadata: Metadata,
    },
}

impl TableMetadataFormat {
    pub fn is_parquet(&self) -> bool {
        matches!(self, Self::Parquet { .. })
    }
}
