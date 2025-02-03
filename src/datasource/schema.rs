use std::{convert::Infallible, str::FromStr};

use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use delta_kernel::schema::StructType;

use super::DataSourceError;

#[derive(Debug, Clone)]
pub struct LogicalTableSchema {
    schema: Schema,
    partition_columns: Vec<String>,
}

impl LogicalTableSchema {
    /// Create a new LogicalTableSchema
    fn new(schema: Schema, partition_columns: Vec<String>) -> Self {
        assert!(
            partition_columns
                .iter()
                .all(|col| schema.index_of(col).is_ok()),
            "partition column is not in schema"
        );

        Self {
            schema,
            partition_columns,
        }
    }

    pub fn as_arrow(&self) -> SchemaRef {
        Arc::new(self.schema.clone())
    }

    fn partition_columns(&self) -> Vec<String> {
        self.partition_columns.clone()
    }

    pub fn project(&self, indices: &[usize]) -> Result<LogicalScanSchema, DataSourceError> {
        let proj_schema = self.as_arrow().project(indices).map_err(|e| {
            DataSourceError::InvalidSchemaProjection(format!("Failed to project schema: {}", e))
        })?;
        let proj_partition_cols = self
            .partition_columns()
            .into_iter()
            .filter_map(|col| match self.schema.index_of(&col) {
                Ok(idx) if indices.contains(&idx) => Some(col),
                _ => None,
            })
            .collect();

        Ok(LogicalScanSchema {
            schema: proj_schema.into(),
            partition_columns: proj_partition_cols,
        })
    }

    pub fn full_projection(&self) -> Result<LogicalScanSchema, DataSourceError> {
        Ok(LogicalScanSchema {
            schema: self.as_arrow(),
            partition_columns: self.partition_columns(),
        })
    }
}

/// Physical schema for a file
#[derive(Debug, Clone)]
pub struct PhysicalFileSchema {
    schema: SchemaRef,
}

#[derive(Debug, Clone)]
pub struct LogicalScanSchema {
    schema: SchemaRef,
    partition_columns: Vec<String>,
}

impl LogicalScanSchema {
    pub fn to_physical_schema(&self) -> PhysicalFileSchema {
        PhysicalFileSchema {
            schema: self.schema.clone(),
        }
    }
}

impl FromStr for LogicalTableSchema {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner: StructType = serde_json::from_str(s).unwrap();
        Ok(Self {
            // inner,
            partition_columns: vec![],
            schema: (&inner).try_into().unwrap(),
        })
    }
}

#[cfg(test)]
mod test {
    use arrow_schema::{DataType, Field};

    use super::*;

    #[test]
    fn simple_projection() {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Boolean, false);
        let schema = Schema::new(vec![field_a, field_b]);

        let table_schema = LogicalTableSchema::new(schema, vec![]);

        let proj1 = table_schema.project(&[0]).unwrap();
        assert_eq!(proj1.schema.fields().len(), 1);
        assert_eq!(proj1.schema.field(0).name(), "a");
        assert_eq!(proj1.partition_columns.len(), 0);

        let proj2 = table_schema.project(&[1]).unwrap();
        assert_eq!(proj2.schema.fields().len(), 1);
        assert_eq!(proj2.schema.field(0).name(), "b");
        assert_eq!(proj2.partition_columns.len(), 0);
    }

    #[test]
    fn project_partition_columns() {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Boolean, false);
        let schema = Schema::new(vec![field_a, field_b]);

        let table_schema = LogicalTableSchema::new(schema, vec!["a".to_string()]);

        let proj1 = table_schema.project(&[0]).unwrap();
        assert_eq!(proj1.schema.fields().len(), 1);
        assert_eq!(proj1.schema.field(0).name(), "a");
        assert_eq!(proj1.partition_columns.len(), 1);
        assert_eq!(proj1.partition_columns[0], "a");

        let proj2 = table_schema.project(&[1]).unwrap();
        assert_eq!(proj2.schema.fields().len(), 1);
        assert_eq!(proj2.schema.field(0).name(), "b");
        assert_eq!(proj2.partition_columns.len(), 0);
    }
}
