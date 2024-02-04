use std::sync::Arc;

use datafusion::prelude::*;
use datafusion_delta_sharing::{catalog::DeltaSharingCatalogList, Profile};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = SessionConfig::new().with_information_schema(true);
    let mut ctx = SessionContext::new_with_config(cfg);

    let profile = Profile::try_from_path("./examples/open-datasets.share")?;
    let delta_sharing_catalog = DeltaSharingCatalogList::try_new(profile).await?;
    ctx.register_catalog_list(Arc::new(delta_sharing_catalog));

    ctx.sql("SELECT * FROM information_schema.tables;")
        .await?
        .show()
        .await?;

    Ok(())
}
