//! Datafusion catalog providers for Delta Sharing.
//!
//! This module provides the catalog integration between Delta Sharing and
//! DataFusion. A Delta Sharing recipient can use its Delta Sharing profile to
//! create a catalog or catalog list in DataFusion and register it in the
//! session context. The shared tables can then be easily queried with SQL.
//!
//! # Example
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # async {
//! # use datafusion::error::DataFusionError;
//! use std::sync::Arc;
//!
//! use datafusion::prelude::*;
//! use datafusion_delta_sharing::{catalog::DeltaSharingCatalogList, Profile};
//!
//! let cfg = SessionConfig::new().with_information_schema(true);
//! let mut ctx = SessionContext::new_with_config(cfg);
//!
//! let profile = Profile::try_from_path("./path/to/profile.share")?;
//! let catalog_list = DeltaSharingCatalogList::try_new(profile).await?;
//! ctx.register_catalog_list(Arc::new(catalog_list));
//!
//! ctx.sql("SELECT * FROM my_share.my_schema.my_table").await?.show().await?;
//! # Ok::<(), DataFusionError>(())};
//! # Ok(()) }
//! ```
use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{schema::SchemaProvider, CatalogList, CatalogProvider},
    datasource::TableProvider,
};

use crate::{
    client::DeltaSharingClient,
    datasource::DeltaSharingTableBuilder,
    profile::Profile,
    securable::{Share, Table},
    DeltaSharingError,
};

/// Datafusion [`CatalogList`] implementation for Delta Sharing.
pub struct DeltaSharingCatalogList {
    shares: HashMap<String, Arc<dyn CatalogProvider>>,
}

impl DeltaSharingCatalogList {
    /// Create a new `DeltaSharingCatalogList` with the given `DeltaSharingProfile`.
    ///
    /// Creating a new `DeltaSharingCatalogList` will eagerly list all
    /// available shares.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # async {
    /// # use datafusion::error::DataFusionError;
    /// use datafusion::catalog::CatalogList;
    /// use datafusion_delta_sharing::{Profile, catalog::DeltaSharingCatalogList};
    ///
    /// let profile = Profile::try_from_path("./path/to/profile.share")?;
    /// let catalog_list = DeltaSharingCatalogList::try_new(profile).await?;
    ///
    /// assert_eq!(catalog_list.catalog_names().len(), 1);
    /// # Ok::<(), DataFusionError>(()) };
    /// # Ok(()) }
    /// ```
    pub async fn try_new(profile: Profile) -> Result<Self, DeltaSharingError> {
        let client = DeltaSharingClient::new(profile.clone());
        let shares = client.list_shares().await?;

        let mut share_map: HashMap<String, Arc<dyn CatalogProvider>> =
            HashMap::with_capacity(shares.len());

        for share in shares {
            let catalog_provider =
                DeltaSharingCatalog::try_new(profile.clone(), share.name()).await?;
            share_map.insert(share.name().to_string(), Arc::new(catalog_provider));
        }

        Ok(Self { shares: share_map })
    }
}

impl CatalogList for DeltaSharingCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        unimplemented!("The DeltaSharingCatalogList is read-only and cannot be modified.")
    }

    fn catalog_names(&self) -> Vec<String> {
        self.shares.keys().cloned().collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.shares.get(name).cloned()
    }
}

/// Datafusion [`CatalogProvider`] implementation for Delta Sharing.
pub struct DeltaSharingCatalog {
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl DeltaSharingCatalog {
    /// Create a new [`DeltaSharingCatalog`] with the given profile and name.
    ///
    /// Creating a new `DeltaSharingCatalogList` will eagerly list all
    /// available shares.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # async {
    /// # use datafusion::error::DataFusionError;
    /// use datafusion::catalog::CatalogProvider;
    /// use datafusion_delta_sharing::{Profile, catalog::DeltaSharingCatalog};
    ///
    /// let profile = Profile::try_from_path("./path/to/profile.share")?;
    /// let catalog = DeltaSharingCatalog::try_new(profile, "my_share").await?;
    ///
    /// assert_eq!(catalog.schema_names().len(), 1);
    /// # Ok::<(), DataFusionError>(()) };
    /// # Ok(()) }
    /// ```
    pub async fn try_new(profile: Profile, share_name: &str) -> Result<Self, DeltaSharingError> {
        let client = DeltaSharingClient::new(profile);

        let share = Share::new(share_name, None);
        let mut schemas = HashMap::new();
        for table in client.list_all_tables(&share).await? {
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

        Ok(Self { schemas: result })
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

/// Datafusion [`SchemaProvider`] implementation for Delta Sharing.
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
        let provider = DeltaSharingTableBuilder::new()
            .with_profile(self.client.profile().clone())
            .with_table(table)
            .build()
            .await
            .ok()?;
        Some(Arc::new(provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.iter().any(|table_name| table_name == name)
    }
}
