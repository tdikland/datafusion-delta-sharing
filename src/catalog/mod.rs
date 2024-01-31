use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{schema::SchemaProvider, CatalogList, CatalogProvider},
    datasource::TableProvider,
};

use crate::{
    datasource::DeltaSharingTableBuilder,
    profile::DeltaSharingProfile,
    securable::{Share, Table},
    {client::profile::DeltaSharingProfile as OldProfile, client::DeltaSharingClient},
};

pub struct DeltaSharingCatalogList {
    shares: HashMap<String, Arc<dyn CatalogProvider>>,
}

impl DeltaSharingCatalogList {
    pub async fn new(new_profile: DeltaSharingProfile) -> Self {
        let profile = OldProfile::from_path("./examples/open-datasets.share");
        let client = DeltaSharingClient::new(profile);
        let shares = client.list_shares().await.unwrap();

        let mut share_map: HashMap<String, Arc<dyn CatalogProvider>> = HashMap::new();
        for share in shares {
            let catalog_provider =
                DeltaSharingCatalog::new(new_profile.clone(), share.name()).await;
            share_map.insert(share.name().to_string(), Arc::new(catalog_provider));
        }

        Self { shares: share_map }
    }
}

impl CatalogList for DeltaSharingCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        unimplemented!()
    }

    fn catalog_names(&self) -> Vec<String> {
        self.shares.keys().cloned().collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.shares.get(name).cloned()
    }
}

pub struct DeltaSharingCatalog {
    client: DeltaSharingClient,
    name: String,
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl DeltaSharingCatalog {
    pub async fn new(_profile: DeltaSharingProfile, share_name: &str) -> Self {
        let profile = OldProfile::from_path("./examples/open-datasets.share");
        let client = DeltaSharingClient::new(profile);

        let share = Share::new(share_name, None);
        let mut schemas = HashMap::new();
        for table in client.list_all_tables(&share).await.unwrap() {
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

    pub fn client(&self) -> &DeltaSharingClient {
        &self.client
    }

    pub fn name(&self) -> &str {
        &self.name
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
    client: DeltaSharingClient,
    share_name: String,
    schema_name: String,
    table_names: Vec<String>,
}

impl DeltaSharingSchema {
    fn new(client: DeltaSharingClient, share_name: String, schema_name: String) -> Self {
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
        let table_builder = DeltaSharingTableBuilder::new(self.client.profile().clone(), table);
        let table = table_builder.build().await.unwrap();
        Some(Arc::new(table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.iter().any(|table_name| table_name == name)
    }
}
