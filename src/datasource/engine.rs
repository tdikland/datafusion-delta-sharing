use std::sync::Arc;

use delta_kernel::{
    EngineInterface, ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler,
};

pub struct DeltaSharingEngine {}

impl DeltaSharingEngine {
    pub fn new() -> Self {
        Self {}
    }
}

impl EngineInterface for DeltaSharingEngine {
    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
        unimplemented!()
    }

    fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
        todo!()
    }

    fn get_json_handler(&self) -> Arc<dyn JsonHandler> {
        todo!()
    }

    fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        todo!()
    }
}


