use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{
        schema::{self, SchemaProvider},
        CatalogProvider,
    },
    datasource::TableProvider,
};

use crate::{
    client::{
        profile::DeltaSharingProfile, rest::RestClient, securable::Table, DeltaSharingClient,
    },
    DeltaSharingTableBuilder,
};

pub struct DeltaSharingCatalog {
    client: RestClient,
    name: String,
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl DeltaSharingCatalog {
    pub async fn new(endpoint: String, token: String, share_name: &str) -> Self {
        let profile = DeltaSharingProfile::new(endpoint, token);
        let client = RestClient::new(profile);

        let mut schemas = HashMap::new();
        for table in client.list_all_tables(share_name).await.unwrap() {
            let schema_provider = schemas
                .entry(table.schema_name().to_string())
                .or_insert_with_key(|schema_name| {
                    DeltaSharingSchema::new(
                        client.clone(),
                        share_name.to_string(),
                        schema_name.to_string(),
                    )
                });
            schema_provider.table_names.push(table.name().to_string());
        }

        let mut result: HashMap<String, Arc<dyn SchemaProvider>> = HashMap::new();
        for (k, v) in schemas {
            result.insert(k, Arc::new(v));
        }

        Self {
            client,
            name: share_name.into(),
            schemas: result,
        }
    }
}

impl CatalogProvider for DeltaSharingCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}

pub struct DeltaSharingSchema {
    client: RestClient,
    share_name: String,
    schema_name: String,
    table_names: Vec<String>,
}

impl DeltaSharingSchema {
    fn new(client: RestClient, share_name: String, schema_name: String) -> Self {
        Self {
            client,
            share_name,
            schema_name,
            table_names: Vec::new(),
        }
    }
}

#[async_trait]
impl SchemaProvider for DeltaSharingSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.table_names.clone()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let table = Table::new(&self.share_name, &self.schema_name, name, None, None);
        let table_builder = DeltaSharingTableBuilder::new(&table);
        let table = table_builder.build().await;
        Some(Arc::new(table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.iter().any(|table_name| table_name == name)
    }
}
