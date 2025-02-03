//! Delta Lake actions in parquet reponse format

use std::collections::HashMap;

use serde::Deserialize;

/// Protocol action
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    /// The minimum version of the protocol that the client must support.
    min_reader_version: i32,
}

impl Protocol {
    /// Retrieve the minimum version of the protocol that the client must
    /// implement to read this table.
    pub fn min_reader_version(&self) -> i32 {
        self.min_reader_version
    }
}

impl Default for Protocol {
    fn default() -> Self {
        Self {
            min_reader_version: 1,
        }
    }
}

/// Representation of the table format.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Format {
    /// The format of the data files backing the shared table.
    provider: String,
}

impl Format {
    /// Retrieve the format provider.
    pub fn provider(&self) -> &str {
        self.provider.as_ref()
    }
}

impl Default for Format {
    fn default() -> Self {
        Self {
            provider: String::from("parquet"),
        }
    }
}

/// Representation of the table metadata.
///
/// The metadata of a table contains all the information required to correctly
/// interpret the data files of the table.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[serde(rename = "metaData")]
pub struct Metadata {
    id: String,
    name: Option<String>,
    description: Option<String>,
    format: Format,
    schema_string: String,
    partition_columns: Vec<String>,
    #[serde(default)]
    configuration: HashMap<String, String>,
    version: Option<String>,
    size: Option<i64>,
    num_files: Option<i64>,
}

impl Metadata {
    /// Retrieve the unique table identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Retrieve the table name provided by the user.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Retrieve the table description provided by the user.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Retrieve the specification of the table format.
    pub fn format(&self) -> &Format {
        &self.format
    }

    /// Retrieve the schema of the table, serialized as a string.
    pub fn schema_string(&self) -> &str {
        &self.schema_string
    }

    /// Retrieve an array of column names that are used to partition the table.
    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    /// Retrieve a map containing configuration options for the table.
    pub fn configuration(&self) -> &HashMap<String, String> {
        &self.configuration
    }

    /// Retrieve the version of the table this metadata corresponds to.
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Retrieve the size of the table in bytes.
    pub fn size(&self) -> Option<i64> {
        self.size
    }

    /// Retrieve the number of files in the table.
    pub fn num_files(&self) -> Option<i64> {
        self.num_files
    }
}

/// Representation of data that is part of a table.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct File {
    pub url: String,
    id: String,
    partition_values: HashMap<String, Option<String>>,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expiration_timestamp: Option<i64>,
}

impl File {
    /// An HTTPS url that a client can use to directly read the data file.
    pub fn url(&self) -> &str {
        self.url.as_ref()
    }

    /// A mutable HTTPS url that a client can use to directly read the data file.
    pub fn url_mut(&mut self) -> &mut String {
        &mut self.url
    }

    /// A unique identifier for the data file in the table.
    pub fn id(&self) -> &str {
        self.id.as_ref()
    }

    /// A map from partition column to value for this file in the table.
    pub fn partition_values(&self) -> HashMap<String, String> {
        self.partition_values
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().unwrap_or_default()))
            .collect()
    }

    /// The size of this file in bytes.
    pub fn size(&self) -> i64 {
        self.size
    }

    /// Summary statistics about the data in this file.
    pub fn stats(&self) -> Option<&str> {
        self.stats.as_deref()
    }

    /// The table version associated with this file.
    pub fn version(&self) -> Option<i64> {
        self.version
    }

    /// The unix timestamp in milliseconds corresponding to the table version
    /// associated with this file.
    pub fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// The unix timestamp in milliseconds corresponding to the expiration of
    /// the url associated with this file.
    pub fn expiration_timestamp(&self) -> Option<i64> {
        self.expiration_timestamp
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Add {
    pub url: String,
    pub id: String,
    pub partition_values: HashMap<String, String>,
    pub size: i64,
    pub timestamp: i64,
    pub version: i32,
    pub stats: Option<String>,
    pub expiration_timestamp: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cdf {
    pub url: String,
    pub id: String,
    pub partition_values: HashMap<String, String>,
    pub size: i64,
    pub timestamp: i64,
    pub version: i32,
    pub expiration_timestamp: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Remove {
    pub url: String,
    pub id: String,
    pub partition_values: HashMap<String, String>,
    pub size: i64,
    pub timestamp: i64,
    pub version: i32,
    pub expiration_timestamp: Option<i64>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn deserialize_protocol_doc() {
        let json = r#"{
          "minReaderVersion": 1
        }"#;
        let protocol: Protocol = serde_json::from_str(json).unwrap();

        assert_eq!(protocol.min_reader_version(), 1);
    }

    #[test]
    fn deserialize_metadata_doc() {
        let json = r#"{
            "partitionColumns": [
              "date"
            ],
            "format": {
              "provider": "parquet"
            },
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}",
            "id": "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
            "configuration": {
              "enableChangeDataFeed": "true"
            },
            "size": 123456,
            "numFiles": 5
        }"#;
        let metadata: Metadata = serde_json::from_str(json).unwrap();

        assert_eq!(metadata.id(), "f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2");
        assert_eq!(metadata.name(), None);
        assert_eq!(metadata.description(), None);
        assert_eq!(metadata.format().provider(), "parquet");
        assert_eq!(
            metadata.schema_string(),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}"
        );
        assert_eq!(metadata.partition_columns(), &["date"]);
        assert_eq!(
            metadata.configuration().get("enableChangeDataFeed"),
            Some(&"true".to_string())
        );
        assert_eq!(metadata.version(), None);
        assert_eq!(metadata.size(), Some(123456));
        assert_eq!(metadata.num_files(), Some(5));
    }

    #[test]
    fn deserialize_file_doc() {
        let json = r#"{
          "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94",
          "id": "591723a8-6a27-4240-a90e-57426f4736d2",
          "size": 573,
          "partitionValues": {
            "date": "2021-04-28"
          },
          "stats": "{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}",
          "expirationTimestamp": 1652140800000
        }"#;
        let file: File = serde_json::from_str(json).unwrap();

        assert_eq!(
            file.url(),
            "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94"
        );
        assert_eq!(file.id(), "591723a8-6a27-4240-a90e-57426f4736d2");
        assert_eq!(
            file.partition_values().get("date"),
            Some(&"2021-04-28".to_string())
        );
        assert_eq!(file.size(), 573);
        assert_eq!(
            file.stats(),
            Some("{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}")
        );
        assert_eq!(file.version(), None);
        assert_eq!(file.timestamp(), None);
        assert_eq!(file.expiration_timestamp(), Some(1652140800000));
    }

    #[test]
    fn deserialize_add_doc() {
        let json = r#"{
            "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94",
            "id": "591723a8-6a27-4240-a90e-57426f4736d2",
            "size": 573,
            "partitionValues": {
            "date": "2021-04-28"
            },
            "timestamp": 1652140800000,
            "version": 1,
            "stats": "{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}",
            "expirationTimestamp": 1652144400000
        }"#;
        let add: Add = serde_json::from_str(json).unwrap();

        assert_eq!(
            add.url,
            "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94"
        );
        assert_eq!(add.id, "591723a8-6a27-4240-a90e-57426f4736d2");
        assert_eq!(
            add.partition_values.get("date"),
            Some(&"2021-04-28".to_string())
        );
        assert_eq!(add.size, 573);
        assert_eq!(add.timestamp, 1652140800000);
        assert_eq!(add.version, 1);
        assert_eq!(
            add.stats,
            Some("{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}".to_string())
        );
        assert_eq!(add.expiration_timestamp, Some(1652144400000));
    }

    #[test]
    fn deserialize_cdf_doc() {
        let json = r#"{
            "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/_change_data/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94",
            "id": "591723a8-6a27-4240-a90e-57426f4736d2",
            "size": 573,
            "partitionValues": {
                "date": "2021-04-28"
            },
            "timestamp": 1652140800000,
            "version": 1,
            "expirationTimestamp": 1652144400000
        }"#;
        let cdf: Cdf = serde_json::from_str(json).unwrap();

        assert_eq!(
            cdf.url,
            "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/_change_data/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94"
        );
        assert_eq!(cdf.id, "591723a8-6a27-4240-a90e-57426f4736d2");
        assert_eq!(
            cdf.partition_values.get("date"),
            Some(&"2021-04-28".to_string())
        );
        assert_eq!(cdf.size, 573);
        assert_eq!(cdf.timestamp, 1652140800000);
        assert_eq!(cdf.version, 1);
        assert_eq!(cdf.expiration_timestamp, Some(1652144400000));
    }

    #[test]
    fn deserialize_remove_doc() {
        let json = r#"{
            "url": "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94",
            "id": "591723a8-6a27-4240-a90e-57426f4736d2",
            "size": 573,
            "partitionValues": {
                "date": "2021-04-28"
            },
            "timestamp": 1652140800000,
            "version": 1,
            "expirationTimestamp": 1652144400000
        }"#;

        let remove: Remove = serde_json::from_str(json).unwrap();

        assert_eq!(
            remove.url,
            "https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table_cdf/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010655Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=dd5d3ba1a179dc7e239d257feed046dccc95000d1aa0479ea6ff36d10d90ec94"
        );
        assert_eq!(remove.id, "591723a8-6a27-4240-a90e-57426f4736d2");
        assert_eq!(
            remove.partition_values.get("date"),
            Some(&"2021-04-28".to_string())
        );
        assert_eq!(remove.size, 573);
        assert_eq!(remove.timestamp, 1652140800000);
        assert_eq!(remove.version, 1);
        assert_eq!(remove.expiration_timestamp, Some(1652144400000));
    }
}
