use std::{any::Any, collections::HashMap, sync::Arc};

use arrow_schema::{Schema, TimeUnit};
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileScanConfig, ParquetExec},
    },
    error::Result,
    execution::object_store::ObjectStoreUrl,
    physical_expr::Partitioning,
    physical_expr::PhysicalSortExpr,
    physical_plan::{metrics::MetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan, Statistics},
    scalar::ScalarValue,
};
use object_store::ObjectMeta;

use crate::{
    client::action::File, datasource::reader::SignedParquetFileReaderFactory, DeltaSharingError,
};

pub struct DeltaSharingScanBuilder {
    schema: Schema,
    projection: Option<Vec<usize>>,
    partition_columns: Vec<String>,
    files: Vec<File>,
}

impl DeltaSharingScanBuilder {
    pub fn new(schema: Schema, partition_columns: Vec<String>) -> Self {
        Self {
            schema,
            partition_columns,
            projection: None,
            files: vec![],
        }
    }

    pub fn with_files(mut self, files: Vec<File>) -> Self {
        self.files = files;
        self
    }

    pub fn with_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn build(self) -> Result<DeltaSharingScan, DeltaSharingError> {
        let file_schema = self.schema.clone();
        let partitioned_files = self
            .files
            .iter()
            .map(|file| {
                build_partitioned_file(file, &file_schema)
                    .expect("Failed to build partitioned file")
            })
            .collect();

        let partition_fields = self
            .partition_columns
            .iter()
            .map(|name| {
                self.schema
                    .field_with_name(name)
                    .cloned()
                    .map_err(|e| DeltaSharingError::other(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let config = FileScanConfig {
            // DeltaSharingScan does not use the ObjectStore abstraction to read files
            // so we set the URL to a local filesystem URL as a cheap default.
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_schema: Arc::new(file_schema.clone()),
            file_groups: vec![partitioned_files],
            statistics: Statistics::new_unknown(&self.schema),
            projection: self.projection,
            limit: None,
            output_ordering: vec![],
            table_partition_cols: partition_fields,
        };
        let pred = None;
        let size_hint = None;

        let exec = ParquetExec::new(config, pred, size_hint)
            .with_enable_bloom_filter(false)
            .with_enable_page_index(false)
            .with_parquet_file_reader_factory(Arc::new(SignedParquetFileReaderFactory::new()));

        Ok(DeltaSharingScan { inner: exec })
    }
}

#[derive(Debug)]
pub struct DeltaSharingScan {
    inner: ParquetExec,
}

impl DisplayAs for DeltaSharingScan {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "DeltaSharingScan"),
            DisplayFormatType::Verbose => write!(f, "DeltaSharingScan"),
        }
    }
}

impl ExecutionPlan for DeltaSharingScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.inner.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::new(self.inner.clone())]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        ExecutionPlan::with_new_children(Arc::new(self.inner.clone()), children)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.inner.schema()))
    }
}

/// Build a partitioned file from a file action and a partition schema
fn build_partitioned_file(
    file: &File,
    partition_schema: &Schema,
) -> Result<PartitionedFile, DeltaSharingError> {
    let partition_values =
        deserialize_partition_values(&file.partition_values(), partition_schema)?;
    let signed_url = file.url().to_owned();

    let partitioned_file = PartitionedFile {
        object_meta: ObjectMeta {
            // Location is not used in the DeltaSharingScan.
            location: Default::default(),
            // The last modified time is not included in the action.
            last_modified: Default::default(),
            size: file.size() as usize,
            e_tag: None,
            version: None,
        },
        partition_values,
        range: None,
        // The signed url is used to download the file.
        // It is passed to the object store via extensions, since the signature
        // in the URL cannot be expressed as part of the object store API.
        extensions: Some(Arc::new(signed_url)),
    };
    Ok(partitioned_file)
}

/// Deserialize partition values from a file into a vector of ScalarValue
/// The partition values are stored in a HashMap with the column name as key and the value as value
/// The partition schema is used to determine the data type of the partition values
fn deserialize_partition_values(
    values: &HashMap<String, String>,
    schema: &Schema,
) -> Result<Vec<ScalarValue>, DeltaSharingError> {
    let mut res = Vec::with_capacity(schema.fields().len());
    for field in schema.all_fields() {
        let partition_value = values.get(field.name()).map(|s| s.as_str());
        let scalar = match partition_value {
            None | Some("") => ScalarValue::try_from(field.data_type())
                .map_err(|e| DeltaSharingError::other(e.to_string()))?,
            Some(str_value) => match &field.data_type() {
                arrow_schema::DataType::Boolean
                | arrow_schema::DataType::Int8
                | arrow_schema::DataType::Int16
                | arrow_schema::DataType::Int32
                | arrow_schema::DataType::Int64
                | arrow_schema::DataType::Float32
                | arrow_schema::DataType::Float64
                | arrow_schema::DataType::Utf8
                | arrow_schema::DataType::Decimal128(_, _)
                | arrow_schema::DataType::Decimal256(_, _)
                | arrow_schema::DataType::Date32
                | arrow_schema::DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    ScalarValue::try_from_string(str_value.to_string(), field.data_type())
                        .map_err(|e| DeltaSharingError::other(e.to_string()))?
                }
                unsupported_datatype => {
                    return Err(DeltaSharingError::other(format!(
                        "Unsupported data type for partition column: {:?}",
                        unsupported_datatype
                    )))
                }
            },
        };
        res.push(scalar);
    }

    Ok(res)
}

#[cfg(test)]
mod test {
    use super::*;

    use arrow_schema::{DataType, Field};

    #[test]
    fn deser_partition_values() {
        let schema = Schema::new(vec![
            Field::new("str", DataType::Utf8, true),
            Field::new("num", DataType::Int32, true),
            Field::new("date", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("bool", DataType::Boolean, true),
        ]);

        let partition_values: HashMap<String, String> = [
            ("str", "foo"),
            ("num", "42"),
            ("date", "1970-01-13"),
            ("ts", "1970-01-14 01:02:03"),
            ("bool", "true"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let res = deserialize_partition_values(&partition_values, &schema).unwrap();

        assert_eq!(res.len(), 5);
        assert_eq!(res[0], ScalarValue::Utf8(Some("foo".to_string())));
        assert_eq!(res[1], ScalarValue::Int32(Some(42)));
        assert_eq!(res[2], ScalarValue::Date32(Some(12)));
        assert_eq!(
            res[3],
            ScalarValue::TimestampMicrosecond(Some(1126923000000), None)
        );
        assert_eq!(res[4], ScalarValue::Boolean(Some(true)));
    }

    #[test]
    fn deser_partition_values_null() {
        let schema = Schema::new(vec![
            Field::new("str", DataType::Utf8, true),
            Field::new("num", DataType::Int32, true),
            Field::new("date", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("bool", DataType::Boolean, true),
        ]);

        let partition_values: HashMap<String, String> = [
            ("str", ""),
            ("num", ""),
            ("date", ""),
            ("ts", ""),
            ("bool", ""),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let res = deserialize_partition_values(&partition_values, &schema).unwrap();

        assert_eq!(res.len(), 5);
        assert_eq!(res[0], ScalarValue::Utf8(None));
        assert_eq!(res[1], ScalarValue::Int32(None));
        assert_eq!(res[2], ScalarValue::Date32(None));
        assert_eq!(res[3], ScalarValue::TimestampMicrosecond(None, None));
        assert_eq!(res[4], ScalarValue::Boolean(None));
    }
}
