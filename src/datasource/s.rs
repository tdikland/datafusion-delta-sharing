use std::{convert::Infallible, str::FromStr};

use std::sync::{Arc, OnceLock};

use arrow_schema::{Schema, SchemaRef};
use delta_kernel::schema::StructType;

#[derive(Debug)]
pub struct LogicalTableSchema {
    inner: StructType,
    arrow_schema: OnceLock<SchemaRef>,
}

impl LogicalTableSchema {
    pub fn as_arrow(&self) -> SchemaRef {
        self.arrow_schema.get_or_init(|| self.to_arrow()).clone()
    }

    fn to_arrow(&self) -> SchemaRef {
        let s: Schema = (&self.inner).try_into().unwrap();
        Arc::new(s)
    }
}

impl FromStr for LogicalTableSchema {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = serde_json::from_str(s).unwrap();
        Ok(Self {
            inner,
            arrow_schema: OnceLock::new(),
        })
    }
}
