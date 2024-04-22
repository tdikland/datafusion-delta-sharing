use delta_kernel::EngineInterface;
// use tokio::runtime::Handle;

struct DeltaSharingLogEngine {
    // rt: Handle,
}

impl DeltaSharingLogEngine {
    pub fn new() -> Self {
        Self {}
    }
}

impl EngineInterface for DeltaSharingLogEngine {
    fn get_expression_handler(&self) -> std::sync::Arc<dyn delta_kernel::ExpressionHandler> {
        todo!()
    }

    fn get_file_system_client(&self) -> std::sync::Arc<dyn delta_kernel::FileSystemClient> {
        todo!()
    }

    fn get_json_handler(&self) -> std::sync::Arc<dyn delta_kernel::JsonHandler> {
        todo!()
    }

    fn get_parquet_handler(&self) -> std::sync::Arc<dyn delta_kernel::ParquetHandler> {
        todo!()
    }
}
